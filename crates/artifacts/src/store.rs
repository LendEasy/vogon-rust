//! Atomic local writes and native AWS SDK S3 persistence. All paths are tenant-qualified.
use crate::{Error, Result};
use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
    primitives::ByteStream,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::OnceCell;

/// Immutable S3 settings; secret fields intentionally have no Debug implementation.
#[derive(Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub credentials: Option<(String, String, Option<String>)>,
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub operation_timeout: Duration,
}
impl S3Config {
    /// Standard jittered exponential retry, bounded by an overall operation deadline.
    pub fn new(bucket: String, region: String) -> Self {
        Self {
            bucket,
            region,
            endpoint: None,
            credentials: None,
            max_attempts: 3,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(30),
        }
    }
    async fn client(&self) -> Client {
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(self.region.clone()))
            .retry_config(
                RetryConfig::standard()
                    .with_max_attempts(self.max_attempts.max(1))
                    .with_initial_backoff(self.initial_backoff)
                    .with_max_backoff(self.max_backoff),
            )
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(self.operation_timeout)
                    .operation_attempt_timeout(Duration::from_secs(10))
                    .connect_timeout(Duration::from_secs(3))
                    .build(),
            );
        if let Some((key, secret, token)) = &self.credentials {
            loader = loader.credentials_provider(Credentials::new(
                key,
                secret,
                token.clone(),
                None,
                "vogon-config",
            ));
        }
        let shared = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.endpoint_url(endpoint).force_path_style(true);
        }
        Client::from_conf(builder.build())
    }
}
/// Shared store configuration/client only; call audio is never held in this process-wide object.
pub struct Stores {
    pub directory: PathBuf,
    pub s3: Option<S3Config>,
    client: OnceCell<Client>,
}
impl Stores {
    /// Build without network access; S3 credentials are resolved asynchronously on first use.
    pub fn new(directory: PathBuf, s3: Option<S3Config>) -> Arc<Self> {
        Arc::new(Self {
            directory,
            s3,
            client: OnceCell::new(),
        })
    }
    /// Only fixed artifact names and safe tenant/call path components are accepted.
    pub fn key(tenant: &str, call: &str, name: &str) -> Result<String> {
        let valid = |s: &str| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        };
        if !valid(tenant)
            || !valid(call)
            || !matches!(
                name,
                "recording.wav" | "audio-events.json" | "frames.csv" | "audio-timeline"
            )
        {
            return Err(Error("invalid_artifact_key"));
        }
        Ok(format!("tenants/{tenant}/calls/{call}/{name}"))
    }
    /// Persist independently to both stores; local success survives a failed cloud upload.
    /// Returns failure categories for observability while call teardown remains fail-open.
    pub async fn put(
        &self,
        tenant: &str,
        call: &str,
        name: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Vec<&'static str> {
        let key = match Self::key(tenant, call, name) {
            Ok(k) => k,
            Err(e) => return vec![e.0],
        };
        let bytes = bytes::Bytes::from(bytes);
        let local = async {
            let path = self.directory.join(&key);
            let parent = path.parent().unwrap();
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|_| Error("artifact_directory_failed"))?;
            let temp = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
            if tokio::fs::write(&temp, &bytes).await.is_err() {
                let _ = tokio::fs::remove_file(&temp).await;
                return Err(Error("artifact_write_failed"));
            }
            if tokio::fs::rename(&temp, &path).await.is_err() {
                let _ = tokio::fs::remove_file(&temp).await;
                return Err(Error("artifact_rename_failed"));
            }
            Ok(())
        };
        let cloud = async {
            if let Some(config) = &self.s3 {
                let client = self.client.get_or_init(|| config.client()).await;
                client
                    .put_object()
                    .bucket(&config.bucket)
                    .key(&key)
                    .content_type(content_type)
                    .body(ByteStream::from(bytes.clone()))
                    .send()
                    .await
                    .map_err(|_| Error("artifact_s3_put_failed"))?;
            }
            Ok(())
        };
        let (a, b) = tokio::join!(local, cloud);
        [a, b]
            .into_iter()
            .filter_map(|r: Result<()>| r.err().map(|e| e.0))
            .collect()
    }
    /// Retrieve local artifacts first, then S3. Missing and failed storage remain distinct.
    pub async fn get(&self, tenant: &str, call: &str, name: &str) -> Result<Option<Vec<u8>>> {
        let key = Self::key(tenant, call, name)?;
        match tokio::fs::read(self.directory.join(&key)).await {
            Ok(bytes) => return Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(Error("artifact_read_failed")),
        }
        let Some(config) = &self.s3 else {
            return Ok(None);
        };
        let client = self.client.get_or_init(|| config.client()).await;
        match client
            .get_object()
            .bucket(&config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(response) => {
                // Uploaded WAVs are bounded by the recorder; external objects must be bounded too.
                if response
                    .content_length()
                    .is_some_and(|n| n > 512 * 1024 * 1024)
                {
                    return Err(Error("artifact_size_limit"));
                }
                let mut stream = response.body;
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|_| Error("artifact_s3_read_failed"))?;
                    if bytes.len() + chunk.len() > 512 * 1024 * 1024 {
                        return Err(Error("artifact_size_limit"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                Ok(Some(bytes))
            }
            Err(e) if e.as_service_error().is_some_and(|e| e.is_no_such_key()) => Ok(None),
            Err(_) => Err(Error("artifact_s3_get_failed")),
        }
    }
    /// Validate that local persistence is writable without touching any call artifact.
    pub async fn probe(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.directory)
            .await
            .map_err(|_| Error("artifact_directory_failed"))?;
        let path = self
            .directory
            .join(format!(".probe-{}", uuid::Uuid::new_v4().simple()));
        tokio::fs::write(&path, b"")
            .await
            .map_err(|_| Error("artifact_write_failed"))?;
        tokio::fs::remove_file(path)
            .await
            .map_err(|_| Error("artifact_cleanup_failed"))
    }
}

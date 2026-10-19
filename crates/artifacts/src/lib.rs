//! Call-local recording and diagnostic artifacts. No clock is advanced by capture.
pub mod recorder;
pub mod store;
pub mod timeline;
/// Stable error code; never contains artifact contents or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error(pub &'static str);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Error {}
/// Public artifact boundary result.
pub type Result<T> = std::result::Result<T, Error>;

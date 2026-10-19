# Security

Report vulnerabilities privately to **security@lendeasy.com**, following the
[Python Vogon security policy](https://github.com/LendEasy/vogon/blob/main/SECURITY.md).
Do not include credentials, customer audio or transcripts in public issues.

Vogon Rust is an experimental migration, not a production-approved deployment.
See [known issues](KNOWN_ISSUES.md) and [remaining modules](MODULES.md).

Keep provider keys in environment variables or a local ignored `.env` file.
The service reads a dotenv only when launched; use `--env .env` in this standalone
checkout. Set `GATEWAY_SERVICE_KEY` for management authentication and
`TWILIO_AUTH_TOKEN` for signed Twilio callbacks. An empty configured key disables
the corresponding check, matching the existing gateway contract. Bind local demos
to loopback and replace the public example tokens before exposing a service.

Treat audio, transcripts, event traces and session records as sensitive data.
Generated validation output is ignored by Git. Module 12 recording and module 13
operational hardening remain pending; do not infer them from test WAV capture or
the basic `/health` endpoint.

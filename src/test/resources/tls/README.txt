Test-only TLS fixtures. NOT secrets: every store uses the password "changeit" and the private keys exist only
so the tests can run a TLS server. Never use these files outside the tests.

server.p12            server key + self-signed certificate for localhost / 127.0.0.1 (valid 100 years)
trust.p12             trust store containing only that certificate
other-server.p12      a different, unrelated key/certificate for localhost
other-trust.p12       trust store containing only the "other" certificate (does not trust server.p12)
wrongname-server.p12  key/certificate issued for wrong.example only
wrongname-trust.p12   trust store that trusts it, so only the host-name check can reject it for "localhost"

Regenerate with keytool -genkeypair -storetype PKCS12 ... (see the commands in docs/SECURITY.md).

# Security: password and TLS

Both are optional and off by default. Without them the server accepts any client that can reach its port, which is
why it listens on `127.0.0.1` only unless you pass `--host`.

## Password (AUTH)
```bash
echo "a-long-random-password" > pw.txt
java -cp target/cache-1.0-SNAPSHOT.jar com.distri.cache.server.ServerMain --password-file pw.txt
```
(or set the `DISTRICACHE_PASSWORD` environment variable). The password is deliberately **not** accepted as a
command-line value, because command lines show up in process listings.

- Until a connection sends `AUTH <password>` (or `AUTH default <password>`), every command except `AUTH` and `QUIT`
  gets `-NOAUTH`. Authentication is per connection.
- Only a SHA-256 digest of the password is kept in memory, and candidates are compared in constant time.
- A wrong guess waits 250 ms before the reply, and the connection is closed after 5 wrong guesses in a row.
  There is **no** limit across connections, so an attacker can open many connections; use a long random password.
- A connection that has not authenticated within 10 seconds is closed (this also covers the TLS handshake).
- The password is never written to the persistence log or printed by `toString()` of the security settings.
- This is one shared password, not user accounts: no roles, no per-command permissions.

Client side:
```java
ClientSecurity security = ClientSecurity.password("a-long-random-password");
try (CacheClient c = new CacheClient("cache.example", 6380, 3000, 5000, security)) { ... }
// every server of a ClusterClient uses the same settings:
ClusterClient.of(security, "10.0.0.1:6380", "10.0.0.2:6380");
```
The client sends `AUTH` every time it opens (or reopens) a connection.

## TLS
Without TLS, a password and all data cross the network in clear text. The server prints a warning if a password is
set, TLS is off and the server is not bound to loopback.

Create a self-signed certificate (fine for a private deployment or a demo; use a real CA for anything else):
```bash
keytool -genkeypair -alias server -dname "CN=cache.example" -ext "san=dns:cache.example,ip:10.0.0.1" \
        -keyalg RSA -keysize 2048 -validity 365 -storetype PKCS12 -keystore server.p12
keytool -exportcert -alias server -keystore server.p12 -file server.cer
keytool -importcert -noprompt -alias server -file server.cer -storetype PKCS12 -keystore trust.p12
echo "keystore-password" > ks.txt
java -cp target/cache-1.0-SNAPSHOT.jar com.distri.cache.server.ServerMain --host 0.0.0.0 \
     --tls-keystore server.p12 --tls-keystore-password-file ks.txt --password-file pw.txt
```
Client side:
```java
ClientSecurity security = ClientSecurity.tls(Path.of("trust.p12"), "trust-store-password".toCharArray(), "the-password");
```
- TLS 1.3 and 1.2 only. The client verifies the certificate against its trust store **and** checks that it was
  issued for the host name it connected to (a certificate for another name is refused even if trusted).
- The keystore password also protects the key (same password for both).
- There is no client-certificate (mutual TLS) support, no certificate reloading (restart to rotate), and no
  revocation checking.

## Not covered
Authorisation beyond one password, audit logging, protection against many connections guessing in parallel,
encryption of the persistence file on disk, and anything between servers (servers do not talk to each other yet).

## Tests
`AuthTest` (server) and `SecureClientTest` (client + server) cover the above. The certificates in
`src/test/resources/tls` are throw-away test fixtures with the password `changeit`; see the README.txt there.

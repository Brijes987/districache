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

## TLS for the replicated (Raft) mode
The replicated server (`ReplicatedServerMain`) can encrypt **both** of its ports: the **client port** and the **peer
port** that the servers use to talk to each other. All four options go together:

```bash
# once per cluster: one key and certificate, shared by the servers. The names/addresses in the certificate must
# include every host name or IP the servers and clients use to reach each other (the host part of --members).
keytool -genkeypair -alias cluster -dname "CN=districache" -ext "san=ip:10.0.0.1,ip:10.0.0.2,ip:10.0.0.3" \
        -keyalg RSA -keysize 2048 -validity 365 -storetype PKCS12 -keystore cluster.p12 -storepass changeit
keytool -exportcert -alias cluster -keystore cluster.p12 -storepass changeit -file cluster.cer
keytool -importcert -noprompt -alias cluster -file cluster.cer -storetype PKCS12 -keystore trust.p12 -storepass changeit
echo changeit > tls.pw

java -cp target/classes com.distri.cache.node.ReplicatedServerMain --id 0 --members 0=10.0.0.1:7000:7100,... --data data0 \
     --peer-secret-file peer.secret --client-secret-file client.secret \
     --tls-keystore cluster.p12 --tls-keystore-password-file tls.pw \
     --tls-truststore trust.p12 --tls-truststore-password-file tls.pw
```
Client side:
```java
SSLContext tls = Tls.clientContext(Path.of("trust.p12"), "changeit".toCharArray());
ReplicatedClient client = new ReplicatedClient(addresses, clientSecret, 15_000, tls);
```
What it does:
- **Peer port: mutual TLS.** A server dialling another server checks that the certificate is trusted and matches the
  address it dialled; the receiving server also requires the dialler to present a certificate its trust store trusts.
  A server whose certificate is not trusted cannot exchange a single Raft message with the others.
- **Client port: the server is authenticated, the client is not** (clients do not need a certificate). The client checks
  the certificate and the host name.
- **The shared-secret handshakes (`--peer-secret-file`, `--client-secret-file`) still run on top**, inside the encrypted
  channel. TLS does not replace them.
- TLS 1.3 and 1.2 only.

Limits, stated plainly:
- All servers share one key (that is how the example above is set up), so a stolen key from any one server lets its
  holder impersonate any server. Separate keys per server need a CA, which this does not set up.
- No certificate reloading (restart to rotate), no revocation checking.
- The metrics endpoint is separate and still has no TLS.
- The cost of TLS (throughput, latency) has **not been measured**.
- Single-server mode and replicated mode use different flags and different code paths; the replicated mode's TLS is
  covered by `ReplicatedTlsTest`.

## Not covered
Authorisation beyond one password, audit logging, protection against many connections guessing in parallel,
and encryption of the persistence files on disk.

## Tests
`AuthTest` (server) and `SecureClientTest` (client + server) cover the single-server mode; `ReplicatedTlsTest` covers the
replicated mode (see below). The certificates in
`src/test/resources/tls` are throw-away test fixtures with the password `changeit`; see the README.txt there.

`ReplicatedTlsTest` (11 tests): a three-server cluster over TLS serves reads and writes and survives killing the leader;
a client without TLS is refused and the servers are unharmed; a client that does not trust the certificate is refused;
a server with a certificate the others do not trust cannot join, never wins an election and receives no entries; the
peer port refuses a connection that presents no certificate and drops plain bytes; a certificate issued for another
name is refused even though it is trusted; a recording proxy shows a marker value in the clear without TLS (control)
and not at all with TLS.
A server that trusts the cluster but presents an untrusted certificate cannot move the leader's term over 3 seconds; and
only TLS 1.3 and 1.2 are enabled (the allowed list is pinned, and a TLS 1.1-only client is refused).

### Hand-made breakages of the TLS code (all caught)
Ten breakages were applied one at a time and `ReplicatedTlsTest` run: outgoing peer links not using TLS; incoming peer
links not using TLS; the peer port not demanding a certificate; host-name checking off; client port not using TLS;
client port demanding client certificates; peers trusting the default CA store instead of the given one; a client that
trusts everything; the client not using TLS; older protocol versions allowed. **All ten are caught.**
Two were found by writing the tests badly first, which is why they are worth stating: the "no certificate" test first
passed for the wrong reason (the server also drops garbage bytes, so it looked refused when it had accepted the
connection), and the old-protocol breakage cannot be seen through a handshake on JDK 23 because the JDK disables those
protocols itself, so that test pins the allowed list instead.

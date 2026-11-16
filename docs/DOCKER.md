# Running a cluster with Docker Compose

    docker compose up --build

starts three servers (`n1`, `n2`, `n3`, Raft with read-index reads), Prometheus (http://localhost:9090) and Grafana
(http://localhost:3000, user `admin`, password `admin`) with a dashboard called DistriCache: leaders, term, commit lag,
request rate and latency, hit ratio, snapshots and log size.

Write a key and read it back through the group (a client inside the same network, because a leader's address is only
reachable from there):

    docker compose run --rm probe

Look at one server's metrics directly: http://localhost:9101/metrics (9102 and 9103 for the others).

Try a failure: `docker compose stop n1` (or whichever has `districache_raft_is_leader 1`), watch the Grafana
"Leaders" panel return to 1 after an election and the term go up by 1, then `docker compose start n1`.

## Limits
- **Not run by me.** I wrote these files where Docker could not run. The YAML and the dashboard JSON parse and the metric
  names in the dashboard exist in the code, but no container has been started from them yet. Results go here when you run it.
- Development only: the two secrets in `docker/secrets` are examples committed to the repository, traffic between servers
  and clients is not encrypted (TLS options exist, docs/SECURITY.md), and the metrics endpoints have no authentication.
- The group is fixed at three servers (no membership changes yet).
- Each server needs a JVM; three of them plus Prometheus and Grafana need a few GB of RAM.

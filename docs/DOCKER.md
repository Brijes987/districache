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

## A reduced trial on a small machine (one Windows laptop, one set of runs)

Machine: Windows 10, 2 CPUs, 4 GB RAM; Docker Desktop's VM had 2 CPUs and 1.795 GiB. Started with
`docker compose -f docker-compose.yml -f docker-compose.small.yml up -d n1 n2 n3 prometheus` (no Grafana; each server has
a 128 MiB Java heap and a 320 MiB container limit). The first image build took about 9.6 minutes (downloads included).

What was measured:

- **Memory** (`docker stats`, one snapshot about 6 minutes after start, idle): n1 48.3 MiB, n2 34.9 MiB, n3 37.7 MiB,
  Prometheus 21.8 MiB.
- **Idle stability, three servers:** after 18 minutes the Raft term was still 5 and one election had been won in total.
- **Writes with no failure** (`RemoteProbe watch`, 20 writes/s for 60 s, all three servers confirmed running): 1,067,
  1,159, 1,140 and 1,089 writes acknowledged in four runs, none failed, every acknowledged write read back correctly.
  Longest gap between two acknowledgements: 3,497 ms in the run whose first write included starting the client JVM, then
  370 ms, 396 ms and 787 ms.
- **Killing the leader under that write load** (`scripts/docker-failover-test.bat`; the killed container was checked to
  be `exited` afterwards). One run per setting:

| Setting | Longest stall | Acknowledged writes | Failed / unknown / missing on read-back | Term |
|---|---|---|---|---|
| default (10 ms tick, election timeout 0.3 to 0.6 s) | 16.0 s (also 8.7 s) | 573 | 0 / 0 / 0 | 8 to 48 in about 80 s |
| `--tick-ms 50` (timeout 1.5 to 3 s) | 2.9 s | 1,079 | 0 / 0 / 0 | ended at 2 |
| `--pre-vote`, run 1 | no new leader for 60+ s | not reported | 1 write failed (outcome unknown); the read-back then crashed | stayed at 1 |
| `--pre-vote`, run 2 | 2.9 s (also 1.5 s) | 1,043 | 0 / 0 / 0 | 1 to 4 |

What this does and does not show:

- No acknowledged write was lost in any run that finished its read-back.
- With the default timings the two surviving servers kept holding elections under load (about 40 in 80 s in the one
  run measured). Longer timeouts gave one election and a 2.9 s stall in the one run measured. I did not find the cause;
  CPU starvation on a 2-CPU VM is a guess, not a finding.
- **Pre-vote run 1 did not recover** from the leader's death within 60 seconds and I do not know why. The same
  scenario recovered in run 2, and in my Linux sandbox with three real server processes it recovered in about 0.6 s with
  the killed node's ports dropped (two runs, one with pre-vote). Because of this, treat pre-vote as untested under
  load in this Docker setup until it is explained. The pre-vote counters added to the metrics
  (`districache_raft_prevote_rounds`, `districache_raft_prevotes_denied`) were read only in run 2 (n1: 6 rounds, 2
  refusals; n3: 4 rounds, 0 refusals).
- Every number is one run on one laptop that was also running Windows and Docker Desktop. They show what happened, not
  what to expect.

A pitfall that invalidated my first attempts: `docker compose run --rm probe` starts any stopped server the probe
depends on, so it silently revived the node I had just killed. For failure tests use `docker compose run --no-deps`
(the script does). I do not know why a stopped server was reported as answering in two early runs; those runs are not
counted above.

## Limits
- **Grafana was not run.** The trial below used the three servers and Prometheus only (Grafana and its dashboard were not started, so the dashboard is untested).
- Development only: the two secrets in `docker/secrets` are examples committed to the repository, traffic between servers
  and clients is not encrypted (TLS options exist, docs/SECURITY.md), and the metrics endpoints have no authentication.
- The group is fixed at three servers (no membership changes yet).
- Each server needs a JVM. On the 1.8 GiB Docker Desktop VM below, three servers and Prometheus fit when each server is limited with docker-compose.small.yml; Grafana was left out.

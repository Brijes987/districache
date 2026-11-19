@echo off
rem Kills the current leader of the Docker cluster about 15 seconds into a 60 second write run and reports what
rem the writer saw. Run from the project folder with the 3 servers up (see docs/DOCKER.md).
setlocal
rem Optional: before running, set EXTRA=-f docker-compose.prevote.yml (or tick50, nofsync) to match how the cluster was started.
set C=docker compose -f docker-compose.yml -f docker-compose.small.yml %EXTRA%
set LEADER=
curl -s -m 3 http://localhost:9101/metrics | findstr /b /c:"districache_raft_is_leader 1" >nul && set LEADER=n1
curl -s -m 3 http://localhost:9102/metrics | findstr /b /c:"districache_raft_is_leader 1" >nul && set LEADER=n2
curl -s -m 3 http://localhost:9103/metrics | findstr /b /c:"districache_raft_is_leader 1" >nul && set LEADER=n3
if "%LEADER%"=="" (
  echo no server reports being the leader right now; wait a few seconds and run this again
  exit /b 1
)
echo leader before the test: %LEADER%
curl -g -s "http://localhost:9090/api/v1/query?query=max(districache_raft_term)"
echo.
start /b cmd /c "ping -n 22 127.0.0.1 >nul & %C% kill %LEADER%"
%C% run --no-deps --rm probe com.distri.cache.node.RemoteProbe watch --servers n1:6381,n2:6381,n3:6381 --client-secret-file /run/secrets/client_secret --seconds 60 --rate 20
echo.
echo state of the killed server after the run (should be exited):
docker inspect -f "{{.Name}} {{.State.Status}} started={{.State.StartedAt}}" districache-%LEADER%-1
curl -g -s "http://localhost:9090/api/v1/query?query=max(districache_raft_term)"
echo.
echo per-server state before the killed one is started again (the killed one prints nothing):
for %%P in (9101 9102 9103) do curl -s -m 3 http://localhost:%%P/metrics | findstr /b "districache_raft_node_id districache_raft_term districache_raft_is_leader districache_raft_leader_id districache_raft_prevote_rounds districache_raft_prevotes_denied districache_raft_failed"
echo.
echo starting %LEADER% again
%C% start %LEADER%

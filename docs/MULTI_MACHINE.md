# Running the replicated cache on separate machines

**Status: the procedure and `RemoteProbe` were tested only with three server processes on one machine. A run across
separate machines has NOT been done, and no result from one is claimed anywhere in this project.** Everything else in the
project (tests, benchmarks, the kill-the-leader demo) shares one machine's CPU, disk and loopback network.

This page is how to do the real thing with three computers (or three VMs/cloud instances/containers on different hosts) and
a fourth place to run the client. The client can run on one of the three if you only have three machines; say so in the
results, because then it shares that machine.

## What you need
- Three machines that can reach each other by IP, each with Java 17+ and this project built (`mvn -q package -DskipTests`
  gives `target/classes`; copy the folder or build on each).
- Two TCP ports per server open between the machines: a *peer* port (servers talk to each other) and a *client* port.
  Example: 7001 and 7101. Open them in the firewall (Windows: `netsh advfirewall firewall add rule name=districache dir=in action=allow protocol=TCP localport=7001,7101`).
- Use the machines' real addresses (for example 192.168.1.11, .12, .13), not `localhost` or `127.0.0.1`: every server gets
  the same `--members` list, and a server uses the entry for another id as the address to dial.
- The same two secret files on all three machines (copy them): a peer secret and a client secret. Any random text.
  Without secrets, anything that can reach the ports can join the group or write to it.

## Start the three servers (one command on each machine)
`MEMBERS` is the same on every machine:

    0=192.168.1.11:7001:7101,1=192.168.1.12:7001:7101,2=192.168.1.13:7001:7101

On machine 0 (use `--id 1` and `--id 2` on the others; use `^` for line breaks on Windows cmd or put it all on one line):

    java -cp target/classes com.distri.cache.node.ReplicatedServerMain --id 0 --members MEMBERS --data data0 \
      --peer-secret-file peer.secret --client-secret-file client.secret

By default writes are forced to disk (fsync). Do not add `--no-fsync` for a measurement you want to describe as durable.
For encryption between machines add the four `--tls-*` options (docs/SECURITY.md); the certificates must name the
machines' addresses, and the probe then needs `--tls-truststore` too.

## Run the probe from another machine
`SERVERS` is the client addresses: `192.168.1.11:7101,192.168.1.12:7101,192.168.1.13:7101`

    java -cp target/classes com.distri.cache.node.RemoteProbe smoke --servers SERVERS --client-secret-file client.secret
    java -cp target/classes com.distri.cache.node.RemoteProbe bench --servers SERVERS --client-secret-file client.secret --clients 8 --seconds 15 --op set
    java -cp target/classes com.distri.cache.node.RemoteProbe bench --servers SERVERS --client-secret-file client.secret --clients 8 --seconds 15 --op get

`smoke` checks that a write can be read back and shows which server (the leader) answered. `bench` reports ops/second and
latency percentiles as seen from the probe machine across the real network. Run each at least twice and write down the
numbers and how they varied.

## Failover test
    java -cp target/classes com.distri.cache.node.RemoteProbe watch --servers SERVERS --client-secret-file client.secret --seconds 60 --rate 20

It writes numbered keys 20 times a second and prints each slow or failed write. When it prints which server answered
(the leader), do ONE of these to that machine, at about the 20-second mark, and note the clock time:
- stop the server process abruptly (Linux/macOS `kill -9 PID`; Windows `taskkill /F /PID PID`);
- pull the network cable or disable the adapter (a partition: harder than a crash, because the old leader keeps running);
- on Linux, add delay or loss to one machine with `tc qdisc add dev eth0 root netem delay 50ms loss 1%` (undo with `tc qdisc del dev eth0 root`), to see what a slow link does.

At the end it reads back every acknowledged write. What to report: the "longest time between two acknowledged writes"
(that is the unavailability seen by one client), whether it says `OK`, and how many writes failed with outcome unknown.
Any `FAILED: ... acknowledged writes missing` is a correctness bug: keep the output and send it.

## What it can and cannot tell you
- It measures what one client sees; it does not know when you pulled the cable (use your note of the time) or why a
  write was slow.
- One run on one set of hardware and one network says nothing about others. Record the machines (CPU, disk, network
  type), Java version and the `--members` layout with the numbers.
- Clock differences between machines do not matter to this system (nothing depends on synchronized clocks, except TTL
  expiry, which `RemoteProbe` does not use).

## Results

| Test | Setup | Result |
|---|---|---|
| smoke | | not measured |
| SET, 8 clients | | not measured |
| GET, 8 clients | | not measured |
| leader stopped (kill -9) during watch | | not measured |
| leader partitioned during watch | | not measured |

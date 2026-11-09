package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** Real servers, real TCP, real files, in one JVM. */
class ReplicatedClusterTest {

    private static Map<String, String> expected(int count, String valuePrefix) {
        Map<String, String> m = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            m.put("key" + i, valuePrefix + i);
        }
        return m;
    }

    @Test
    void aGroupElectsALeaderAndServesSetGetAndDelete() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(10_000)) {
            cluster.startAll();
            cluster.awaitLeader(5_000);
            assertEquals(Optional.empty(), client.get("missing"));
            client.set("a", "1");
            assertEquals(Optional.of("1"), client.get("a"));
            client.set("a", "2");
            assertEquals(Optional.of("2"), client.get("a"));
            assertTrue(client.delete("a"));
            assertFalse(client.delete("a"));
            assertEquals(Optional.empty(), client.get("a"));
            client.set("unicode", "välue ☃");
            assertEquals(Optional.of("välue ☃"), client.get("unicode"));
        }
    }

    @Test
    void aTimeToLiveEndsAnEntryOnEveryServer() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(10_000)) {
            cluster.startAll();
            cluster.awaitLeader(5_000);
            client.set("short", "x", 150);
            assertEquals(Optional.of("x"), client.get("short"));
            Thread.sleep(400);
            assertEquals(Optional.empty(), client.get("short"));
        }
    }

    @Test
    void aFollowerAnswersNotLeaderAndNamesTheLeadersClientAddress() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            int follower = (leader + 1) % 3;
            TestCluster.await(() -> cluster.servers[follower].commitIndex() >= 1, 5_000, "the follower to learn the leader");
            try (TestCluster.Raw raw = new TestCluster.Raw(cluster.members.get(follower).clientPort(), null)) {
                ClientProtocol.Response r = raw.call(new ClientProtocol.Request(ClientProtocol.SET, 1, 1, "k", "v", -1));
                assertEquals(ClientProtocol.NOT_LEADER, r.status());
                assertEquals("127.0.0.1:" + cluster.members.get(leader).clientPort(), r.hint());
            }
        }
    }

    @Test
    void aRepeatedRequestNumberIsAnsweredFromMemoryNotExecutedAgain() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            try (TestCluster.Raw raw = new TestCluster.Raw(cluster.members.get(leader).clientPort(), null)) {
                long client = 4242;
                assertEquals(ClientProtocol.OK, raw.call(new ClientProtocol.Request(ClientProtocol.SET, client, 1, "k", "v", -1)).status());
                ClientProtocol.Response first = raw.call(new ClientProtocol.Request(ClientProtocol.DEL, client, 2, "k", "", -1));
                ClientProtocol.Response retry = raw.call(new ClientProtocol.Request(ClientProtocol.DEL, client, 2, "k", "", -1));
                assertTrue(first.found());
                assertTrue(retry.found(), "the retry gets the original answer, not 'already gone'");
                // another client writes the key; a further retry must not delete it
                raw.call(new ClientProtocol.Request(ClientProtocol.SET, 7, 1, "k", "other", -1));
                raw.call(new ClientProtocol.Request(ClientProtocol.DEL, client, 2, "k", "", -1));
                ClientProtocol.Response get = raw.call(new ClientProtocol.Request(ClientProtocol.GET, 8, 1, "k", "", -1));
                assertEquals("other", get.value());
                ClientProtocol.Response stale = raw.call(new ClientProtocol.Request(ClientProtocol.SET, client, 1, "k", "evil", -1));
                assertEquals(ClientProtocol.STALE, stale.status());
            }
        }
    }

    @Test
    void acknowledgedWritesSurviveKillingTheLeaderAndTheClientNeverNoticesMoreThanAPause() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(20_000)) {
            cluster.startAll();
            int oldLeader = cluster.awaitLeader(5_000);
            for (int i = 0; i < 40; i++) {
                client.set("key" + i, "v" + i);
            }
            cluster.kill(oldLeader);
            for (int i = 40; i < 80; i++) {
                client.set("key" + i, "v" + i); // must keep working through the election
            }
            int newLeader = cluster.awaitLeader(5_000);
            assertTrue(newLeader != oldLeader);
            for (int i = 0; i < 80; i++) {
                assertEquals(Optional.of("v" + i), client.get("key" + i), "key" + i);
            }
        }
    }

    @Test
    void aKilledServerRestartedFromItsFilesCatchesUp() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(20_000)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            int victim = (leader + 1) % 3;
            for (int i = 0; i < 20; i++) {
                client.set("key" + i, "v" + i);
            }
            cluster.kill(victim);
            for (int i = 20; i < 50; i++) {
                client.set("key" + i, "v" + i);
            }
            cluster.start(victim);
            Map<String, String> want = expected(50, "v");
            TestCluster.await(() -> {
                try {
                    return want.equals(cluster.servers[victim].localContents());
                } catch (InterruptedException e) {
                    return false;
                }
            }, 10_000, "the restarted server to hold all 50 entries");
        }
    }

    @Test
    void theWholeGroupCanBeStoppedAndStartedAgainWithItsDataAndRealFsync() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, true)) {
            cluster.startAll();
            cluster.awaitLeader(5_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 25; i++) {
                    client.set("key" + i, "v" + i);
                }
            }
            for (int i = 0; i < 3; i++) {
                cluster.kill(i);
            }
            cluster.startAll();
            cluster.awaitLeader(10_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 25; i++) {
                    assertEquals(Optional.of("v" + i), client.get("key" + i), "key" + i);
                }
            }
        }
    }

    @Test
    void withoutAMajorityNothingIsAcknowledgedAndRecoveryRestoresService() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                client.set("before", "1");
            }
            for (int i = 0; i < 3; i++) {
                if (i != leader) {
                    cluster.kill(i);
                }
            }
            try (ReplicatedClient impatient = cluster.client(1_500)) {
                ReplicatedClientException e = assertThrows(ReplicatedClientException.class,
                        () -> impatient.set("during", "2"));
                assertTrue(e.getMessage().contains("no answer"), e.getMessage());
                assertThrows(ReplicatedClientException.class, () -> impatient.get("before"),
                        "reads need a majority too: a lone leader must not answer from its own copy");
            }
            cluster.start((leader + 1) % 3);
            try (ReplicatedClient client = cluster.client(20_000)) {
                client.set("after", "3");
                assertEquals(Optional.of("1"), client.get("before"));
                assertEquals(Optional.of("3"), client.get("after"));
            }
        }
    }

    @Test
    void secretsAreRequiredAndAWrongOneIsRefused() throws Exception {
        byte[] peer = "peer-secret".getBytes();
        byte[] clients = "client-secret".getBytes();
        try (TestCluster cluster = new TestCluster(3, peer, clients, false)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            try (ReplicatedClient ok = cluster.client(10_000)) {
                ok.set("k", "v");
                assertEquals(Optional.of("v"), ok.get("k"));
            }
            try (ReplicatedClient wrong = new ReplicatedClient(cluster.clientAddresses(), "nope".getBytes(), 1_000)) {
                assertThrows(ReplicatedClientException.class, () -> wrong.get("k"));
            }
            try (ReplicatedClient none = new ReplicatedClient(cluster.clientAddresses(), null, 1_000)) {
                assertThrows(ReplicatedClientException.class, () -> none.get("k"));
            }
            // the connecting side only learns the proof was refused when the server closes the connection
            assertThrows(IOException.class, () -> {
                try (TestCluster.Raw raw = new TestCluster.Raw(cluster.members.get(leader).clientPort(), "nope".getBytes())) {
                    raw.call(new ClientProtocol.Request(ClientProtocol.GET, 1, 1, "k", "", -1));
                }
            });
        }
    }

    @Test
    void garbageOnEitherPortDoesNotHurtTheGroup() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(10_000)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            byte[][] junk = {
                new byte[] {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, 1, 2, 3},
                new byte[] {0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'},
                new byte[200],
                "GET / HTTP/1.1\r\n\r\n".getBytes()
            };
            for (int port : new int[] {cluster.members.get(leader).peerPort(), cluster.members.get(leader).clientPort()}) {
                for (byte[] bytes : junk) {
                    try (Socket s = new Socket()) {
                        s.connect(new InetSocketAddress("127.0.0.1", port), 500);
                        s.getOutputStream().write(bytes);
                        s.getOutputStream().flush();
                        s.setSoTimeout(300);
                        try {
                            s.getInputStream().read(new byte[64]);
                        } catch (IOException expected) {
                            // closed or silent: both fine
                        }
                    }
                }
            }
            client.set("still", "works");
            assertEquals(Optional.of("works"), client.get("still"));
            for (int i = 0; i < 3; i++) {
                assertFalse(cluster.servers[i].failed(), "server " + i);
            }
        }
    }

    @Test
    void anOversizedKeyIsRejectedWithoutDisturbingTheGroup() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(5_000)) {
            cluster.startAll();
            cluster.awaitLeader(5_000);
            String huge = "k".repeat(ClientProtocol.MAX_KEY_BYTES + 1);
            assertThrows(ReplicatedClientException.class, () -> client.set(huge, "v"));
            client.set("fine", "v");
            assertEquals(Optional.of("v"), client.get("fine"));
        }
    }

    @Test
    void twoClientsOnDifferentServersSeeEachOthersWrites() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.startAll();
            cluster.awaitLeader(5_000);
            List<InetSocketAddress> all = cluster.clientAddresses();
            try (ReplicatedClient a = new ReplicatedClient(List.of(all.get(0), all.get(1), all.get(2)), null, 10_000);
                    ReplicatedClient b = new ReplicatedClient(List.of(all.get(2), all.get(1), all.get(0)), null, 10_000)) {
                a.set("shared", "from-a");
                assertEquals(Optional.of("from-a"), b.get("shared"));
                b.set("shared", "from-b");
                assertEquals(Optional.of("from-b"), a.get("shared"));
            }
        }
    }
}

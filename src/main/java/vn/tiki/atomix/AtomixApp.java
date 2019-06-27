package vn.tiki.atomix;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionBuilder;
import io.atomix.core.election.Leadership;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class AtomixApp {

    public static Map<Integer, String > map = new HashMap<>();

    static {
        map.put(5001, "member1");
        map.put(5002, "member2");
        map.put(5003, "member3");
    }

    public static final String LOCAL_HOST = "127.0.0.1";

    private int port;
    Atomix atomix;

    public static void main(String[] args) {
        int currentNodePort = 5001;
        if (args.length > 0) {
            currentNodePort = Integer.parseInt(args[0]);
        }

        //kryo.register(io.atomix.cluster.MemberId.class);

        AtomixApp atomixApp = new AtomixApp(currentNodePort);
        atomixApp.beginAtomix();

        atomixApp.leaderElectTest();

        atomixApp.log("Started");
    }

    public AtomixApp(int port) {
        this.port = port;
    }

    public void leaderElectTest() {
        LeaderElection<MemberId> election = atomix.<MemberId>leaderElectionBuilder("my-election")
                        .withProtocol(MultiRaftProtocol.builder()
                        .withReadConsistency(ReadConsistency.LINEARIZABLE)
                        .build())
                .build();

        MemberId localMemberId = atomix.getMembershipService().getLocalMember().id();

        Leadership<MemberId> leadership = election.run(localMemberId);

        log(leadership.leader().id().toString());

    }


    public void beginAtomix() {
       log("Running");
        this.atomix = Atomix.builder()
                .withMemberId(map.get(port))
                .withAddress(LOCAL_HOST, port)
                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
                        .withNodes(
                                Node.builder()
                                        .withId("member1")
                                        .withAddress(LOCAL_HOST, 5001)
                                        .build(),
                                Node.builder()
                                        .withId("member2")
                                        .withAddress(LOCAL_HOST, 5002)
                                        .build(),
                                Node.builder()
                                        .withId("member3")
                                        .withAddress(LOCAL_HOST, 5003)
                                        .build())
                        .build())
                .withManagementGroup(RaftPartitionGroup.builder("system")
                        .withNumPartitions(1)
                        .withMembers("member1", "member2", "member3")
                        .build())
                .withPartitionGroups(RaftPartitionGroup.builder("raft")
                        .withPartitionSize(3)
                        .withNumPartitions(3)
                        .withMembers("member1", "member2", "member3")
                        .build())
                .build();

        atomix.start().join();
    }

    private void log(String msg) {
        String timeStamp = new SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
        System.out.println(timeStamp + " Node " + port + " --> " + msg);
    }
}

package storm;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.topology.TopologyBuilder;

public class StackingTopology {

    private final int NUM_SPOUT = 1;
    private final int NUM_CNNBOLT = 1;
    private final int NUM_LSTMBOLT = 2;
    private final int NUM_GRUBOLT = 2;
    private final int NUM_FINALBOLT = 1;
    private final int NUM_OUTPUTBOLT = 1;

    public static void main(String[] args) throws Exception {
        int numSecs = 300;
        if (args != null && args.length > 0) {
            numSecs = Integer.valueOf(args[0]);
        }

        String topologyName = "Storm-Stacking";
        if (args != null && args.length > 1) {
            topologyName = args[1];
        }

        Config config = new Config();
        config.setNumWorkers(3);

        Cluster cluster = new Cluster(config);

        try {
            cluster.submitTopology(topologyName, config, new StackingTopology().topology());
            Thread.sleep(numSecs * 1000);
        } finally {
            cluster.killTopology(topologyName);
        }

        System.exit(0);
    }

    public StormTopology topology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("input-spout", new InputSpout(), NUM_SPOUT);
        topologyBuilder.setBolt("cnn-bolt", new CNNBolt(), NUM_CNNBOLT).shuffleGrouping("input-spout");
        topologyBuilder.setBolt("lstm-bolt", new LSTMBolt(), NUM_LSTMBOLT).shuffleGrouping("cnn-bolt");
        topologyBuilder.setBolt("gru-bolt", new GRUBolt(), NUM_GRUBOLT).shuffleGrouping("lstm-bolt");
        topologyBuilder.setBolt("final-bolt", new StackingBolt(), NUM_FINALBOLT).shuffleGrouping("gru-bolt");
        topologyBuilder.setBolt("output-bolt", new OutputBolt(), NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");

        return topologyBuilder.createTopology();
    }
}

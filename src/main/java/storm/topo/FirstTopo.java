package storm.topo;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.detect.CNNBolt;
import storm.detect.FinalFirstBolt;
import storm.detect.GRUBolt;
import storm.detect.LSTMBolt;
import storm.input.InputSpout;
import storm.output.OutputBolt;

public class FirstTopo {
    private final int NUM_SPOUT = 1;
    private final int NUM_CNNBOLT = 1;
    private final int NUM_LSTMBOLT = 2;
    private final int NUM_GRUBOLT = 2;
    private final int NUM_FINALBOLT = 1;
    private final int NUM_OUTPUTBOLT = 1;

    public StormTopology topology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("input-spout", new InputSpout(), NUM_SPOUT);
        topologyBuilder.setBolt("cnn-bolt", new CNNBolt(), NUM_CNNBOLT).allGrouping("input-spout");
        topologyBuilder.setBolt("lstm-bolt", new LSTMBolt(), NUM_LSTMBOLT).allGrouping("input-spout");
        topologyBuilder.setBolt("gru-bolt", new GRUBolt(), NUM_GRUBOLT).allGrouping("input-spout");
        topologyBuilder.setBolt("final-bolt", new FinalFirstBolt(), NUM_FINALBOLT)
                .fieldsGrouping("cnn-bolt", new Fields("url"))
                .fieldsGrouping("lstm-bolt", new Fields("url"))
                .fieldsGrouping("gru-bolt", new Fields("url"));
        topologyBuilder.setBolt("output-bolt", new OutputBolt(), NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");

        return topologyBuilder.createTopology();
    }
}

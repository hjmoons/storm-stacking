package storm.topo;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.detect.IS.CNNBolt;
import storm.detect.IS.GRUBolt;
import storm.detect.IS.LSTMBolt;
import storm.detect.IS.StackingBolt;
import storm.input.InputSpout;
import storm.output.OutputBolt;

public class ISTopo {
    private final int NUM_SPOUT = 1;
    private final int NUM_CNNBOLT = 1;
    private final int NUM_LSTMBOLT = 2;
    private final int NUM_GRUBOLT = 2;
    private final int NUM_FINALBOLT = 1;
    private final int NUM_OUTPUTBOLT = 1;

    public StormTopology topology(int trans_time) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("input-spout", new InputSpout(trans_time), NUM_SPOUT);
        topologyBuilder.setBolt("cnn-bolt", new CNNBolt(), NUM_CNNBOLT).shuffleGrouping("input-spout");
        topologyBuilder.setBolt("lstm-bolt", new LSTMBolt(), NUM_LSTMBOLT).shuffleGrouping("input-spout");
        topologyBuilder.setBolt("gru-bolt", new GRUBolt(), NUM_GRUBOLT).shuffleGrouping("input-spout");
        topologyBuilder.setBolt("final-bolt", new StackingBolt(), NUM_FINALBOLT)
                .fieldsGrouping("cnn-bolt", new Fields("id"))
                .fieldsGrouping("lstm-bolt", new Fields("id"))
                .fieldsGrouping("gru-bolt", new Fields("id"));
        topologyBuilder.setBolt("output-bolt", new OutputBolt(), NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");

        return topologyBuilder.createTopology();
    }
}

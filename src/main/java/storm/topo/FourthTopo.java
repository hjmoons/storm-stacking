package storm.topo;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import storm.detect.v4.CNNBolt;
import storm.detect.v4.FinalFourthBolt;
import storm.detect.v4.GRUBolt;
import storm.detect.v4.LSTMBolt;
import storm.input.InputSpout;
import storm.output.OutputBolt;

public class FourthTopo {
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
        topologyBuilder.setBolt("lstm-bolt", new LSTMBolt(), NUM_LSTMBOLT).shuffleGrouping("cnn-bolt");
        topologyBuilder.setBolt("gru-bolt", new GRUBolt(), NUM_GRUBOLT).shuffleGrouping("lstm-bolt");
        topologyBuilder.setBolt("final-bolt", new FinalFourthBolt(), NUM_FINALBOLT).shuffleGrouping("gru-bolt");
        topologyBuilder.setBolt("output-bolt", new OutputBolt(), NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");
        return topologyBuilder.createTopology();
    }
}

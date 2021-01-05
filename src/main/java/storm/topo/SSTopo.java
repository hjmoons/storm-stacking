package storm.topo;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import storm.detect.SS.IntegratedStackingBolt;
import storm.input.InputSpout;
import storm.output.OutputBolt;

public class SSTopo {
    private final int NUM_SPOUT = 1;
    private final int NUM_FINALBOLT = 6;
    private final int NUM_OUTPUTBOLT = 1;

    public StormTopology topology(int trans_time) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("input-spout", new InputSpout(trans_time), NUM_SPOUT);
        topologyBuilder.setBolt("final-bolt", new IntegratedStackingBolt(), NUM_FINALBOLT).shuffleGrouping("input-spout");
        topologyBuilder.setBolt("output-bolt", new OutputBolt(), NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");

        return topologyBuilder.createTopology();
    }
}

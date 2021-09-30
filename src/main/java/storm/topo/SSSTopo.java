package storm.topo;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import storm.detect.SSS.StackingBolt;
import storm.detect.SSS.BaseModelBolt;
import storm.input.InputSpout;
import storm.output.OutputBolt;

public class SSSTopo {
    private final int NUM_SPOUT = 1;
    private final int NUM_LEVEL0BOLT = 5;
    private final int NUM_FINALBOLT = 1;
    private final int NUM_OUTPUTBOLT = 1;

    public StormTopology topology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("input-spout", new InputSpout(), NUM_SPOUT);
        topologyBuilder.setBolt("base-model-bolt", new BaseModelBolt(), NUM_LEVEL0BOLT).shuffleGrouping("input-spout");
        topologyBuilder.setBolt("final-bolt", new StackingBolt(), NUM_FINALBOLT).shuffleGrouping("base-model-bolt");
        topologyBuilder.setBolt("output-bolt", new OutputBolt(), NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");
        return topologyBuilder.createTopology();
    }
}

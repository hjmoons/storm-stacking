package storm.topo;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import storm.detect.v3.FinalThirdBolt;
import storm.detect.v3.Level0Bolt;
import storm.input.InputSpout;
import storm.output.OutputBolt;

public class ThirdTopo {
    private final int NUM_SPOUT = 1;
    private final int NUM_LEVEL0BOLT = 3;
    private final int NUM_FINALBOLT = 3;
    private final int NUM_OUTPUTBOLT = 1;

    public StormTopology topology(int trans_time) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("input-spout", new InputSpout(trans_time), NUM_SPOUT);
        topologyBuilder.setBolt("level0-bolt", new Level0Bolt(), NUM_LEVEL0BOLT).shuffleGrouping("input-spout");
        topologyBuilder.setBolt("final-bolt", new FinalThirdBolt(), NUM_FINALBOLT).shuffleGrouping("level0-bolt");
        topologyBuilder.setBolt("output-bolt", new OutputBolt(), NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");
        return topologyBuilder.createTopology();
    }
}

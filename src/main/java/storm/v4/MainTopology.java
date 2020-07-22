package storm.v4;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import storm.v4.input.InputSpout;
import storm.v4.level0.CNNBolt;
import storm.v4.level0.GRUBolt;
import storm.v4.level0.LSTMBolt;
import storm.v4.level1.FinalBolt;
import storm.v4.output.OutputBolt;

import java.util.Map;

public class MainTopology {
    private Log log = LogFactory.getLog(MainTopology.class);
    private String topologyName = "StackingModel";

    private final int NUM_WORKERS = 8;
    private final int NUM_SPOUT = 1;
    private final int NUM_CNNBOLT = 1;
    private final int NUM_LSTMBOLT = 2;
    private final int NUM_GRUBOLT = 2;
    private final int NUM_FINALBOLT = 1;
    private final int NUM_OUTPUTBOLT = 1;

    public void topology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("input-spout", new InputSpout(), NUM_SPOUT);
        topologyBuilder.setBolt("cnn-bolt",  new CNNBolt(), NUM_CNNBOLT).shuffleGrouping("input-spout");
        topologyBuilder.setBolt("lstm-bolt", new LSTMBolt(), NUM_LSTMBOLT).shuffleGrouping("cnn-bolt");
        topologyBuilder.setBolt("gru-bolt", new GRUBolt(), NUM_GRUBOLT).shuffleGrouping("lstm-bolt");
        topologyBuilder.setBolt("final-bolt", new FinalBolt(), NUM_FINALBOLT).shuffleGrouping("gru-bolt");
        topologyBuilder.setBolt("output-bolt", new OutputBolt(), NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");

        Config config = new Config();
        config.setNumWorkers(NUM_WORKERS);

        try {
            StormSubmitter.submitTopology(topologyName, config, topologyBuilder.createTopology());

            Thread.sleep(60 * 60 * 1000);

            Map<String, Object> conf = Utils.readStormConfig();
            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(0);
            client.killTopologyWithOpts(topologyName, killOpts);

        } catch (AlreadyAliveException e) {
            log.info(e.get_msg());
        } catch (InvalidTopologyException e) {
            log.info(e.get_msg());
        } catch (AuthorizationException e) {
            log.info(e.get_msg());
        } catch (NotAliveException e) {
            log.info(e.get_msg());
        } catch (TException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new MainTopology().topology();
    }
}

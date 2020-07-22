package storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import storm.input.InputSpout;
import storm.level0.CNNBolt;
import storm.level0.GRUBolt;
import storm.level0.LSTMBolt;
import storm.level1.FinalBolt;
import storm.output.OutputBolt;

import java.util.Map;

public class MainTopology {
    private String topologyName = "StackingModel";

    private final int NUM_WORKERS = 8;
    private final int NUM_SPOUT = 1;
    private final int NUM_CNNBOLT = 1;
    private final int NUM_LSTMBOLT = 1;
    private final int NUM_GRUBOLT = 1;
    private final int NUM_FINALBOLT = 1;
    private final int NUM_OUTPUTBOLT = 1;

    public void topology() {
        InputSpout inputSpout = new InputSpout();
        CNNBolt cnnBolt = new CNNBolt();
        LSTMBolt lstmBolt = new LSTMBolt();
        GRUBolt gruBolt = new GRUBolt();
        FinalBolt finalBolt = new FinalBolt();
        OutputBolt outputBolt = new OutputBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("input-spout", inputSpout, NUM_SPOUT);
        topologyBuilder.setBolt("cnn-bolt", cnnBolt, NUM_CNNBOLT).allGrouping("input-spout");
        topologyBuilder.setBolt("lstm-bolt", lstmBolt, NUM_LSTMBOLT).allGrouping("input-spout");
        topologyBuilder.setBolt("gru-bolt", gruBolt, NUM_GRUBOLT).allGrouping("input-spout");
        topologyBuilder.setBolt("final-bolt", finalBolt, NUM_FINALBOLT).fieldsGrouping("cnn-bolt", new Fields("level0")).fieldsGrouping("lstm-bolt", new Fields("level0")).fieldsGrouping("gru-bolt", new Fields("level0"));
        topologyBuilder.setBolt("output-bolt", outputBolt, NUM_OUTPUTBOLT).shuffleGrouping("final-bolt");

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
            //LOG.info(e.get_msg());
        } catch (InvalidTopologyException e) {
            //LOG.info(e.get_msg());
        } catch (AuthorizationException e) {
            //LOG.info(e.get_msg());
        } catch (NotAliveException e) {
            //LOG.info(e.get_msg());
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

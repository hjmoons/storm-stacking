package storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import storm.topo.ISTopo;
import storm.topo.SISTopo;
import storm.topo.SSTopo;
import storm.topo.SSSTopo;

import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String topologyName = args[0];
        String topologyNumber = args[1];
        StormTopology stormTopology = null;

        switch (Integer.parseInt(topologyNumber)) {
            case 1:
                stormTopology = new ISTopo().topology();
                break;
            case 2:
                stormTopology = new SSTopo().topology();
                break;
            case 3:
                stormTopology = new SSSTopo().topology();
                break;
            case 4:
                stormTopology = new SISTopo().topology();
                break;
            default:
                break;
        }

        Config config = new Config();
        config.setNumWorkers(3);

        try {
            StormSubmitter.submitTopology(topologyName, config, stormTopology);

            Thread.sleep(60 * 60 * 1000);

            Map<String, Object> conf = Utils.readStormConfig();
            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(0);
            client.killTopologyWithOpts(topologyName, killOpts);

        } catch (AlreadyAliveException e) {

        } catch (InvalidTopologyException e) {

        } catch (AuthorizationException e) {

        } catch (NotAliveException e) {

        } catch (TException e) {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

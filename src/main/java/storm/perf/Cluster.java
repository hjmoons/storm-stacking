package storm.perf;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * 성능 측정을 위한 토폴로지 클래스
 */
public class Cluster {
    LocalCluster _local = null;
    Nimbus.Client _client = null;

    public Cluster(Map conf) {
        Map clusterConf = Utils.readStormConfig();
        if (conf != null) {
            clusterConf.putAll(conf);
        }
        Boolean isLocal = (Boolean)clusterConf.get("run.local");
        if (isLocal != null && isLocal) {
            _local = new LocalCluster();
        } else {
            _client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        }
    }

    public ClusterSummary getClusterInfo() throws Exception {
        if (_local != null) {
            return _local.getClusterInfo();
        } else {
            return _client.getClusterInfo();
        }
    }

    public TopologyInfo getTopologyInfo(String id) throws Exception {
        if (_local != null) {
            return _local.getTopologyInfo(id);
        } else {
            return _client.getTopologyInfo(id);
        }
    }

    public void killTopology(String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        if (_local != null) {
            _local.killTopologyWithOpts(name, opts);
        } else {
            _client.killTopologyWithOpts(name, opts);
        }
    }

    public void submitTopology(String name, Map stormConf, StormTopology topology) throws Exception {
        if (_local != null) {
            _local.submitTopology(name, stormConf, topology);
        } else {
            StormSubmitter.submitTopology(name, stormConf, topology);
            setupShutdownHook(name);
        }
    }

    public void setupShutdownHook(String name) {
        Map clusterConf = Utils.readStormConfig();
        final Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    killTopology(name);
                    System.out.println("Killed Topology");
                } catch (Exception var2) {
                    var2.printStackTrace();
                }

            }
        });
    }

    public boolean isLocal() {
        return _local != null;
    }
}

package storm;

import org.apache.storm.Config;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.utils.Utils;
import storm.topo.ISTopo;
import storm.topo.SISTopo;
import storm.topo.SSTopo;
import storm.topo.SSSTopo;

import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        String topologyName = args[0];
        String configPath = args[1];
        String topologyNumber = args[2];
        int executionTime = Integer.parseInt(args[3]);
        int transmitTime = Integer.parseInt(args[4]);

        Map<String, Object> topoConf = Utils.findAndReadConfigFile(configPath);
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        topoConf.put(Config.TOPOLOGY_NAME, topologyName);

        switch (Integer.parseInt(topologyNumber)) {
            case 1:
                Helper.runOnClusterAndPrintMetrics(executionTime * 60, topologyName, topoConf, new ISTopo().topology(transmitTime));
                break;
            case 2:
                Helper.runOnClusterAndPrintMetrics(executionTime * 60, topologyName, topoConf, new SSTopo().topology(transmitTime));
                break;
            case 3:
                Helper.runOnClusterAndPrintMetrics(executionTime * 60, topologyName, topoConf, new SSSTopo().topology(transmitTime));
                break;
            case 4:
                Helper.runOnClusterAndPrintMetrics(executionTime * 60, topologyName, topoConf, new SISTopo().topology(transmitTime));
                break;
            default:
                break;
        }
    }
}

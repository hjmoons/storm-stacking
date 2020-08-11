package storm;

import org.apache.storm.Config;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.utils.Utils;
import storm.topo.FirstTopo;
import storm.topo.FourthTopo;
import storm.topo.SecondTopo;
import storm.topo.ThirdTopo;

import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        String topologyName = args[0];
        String configPath = args[1];
        String topologyNumber = args[2];

        Map<String, Object> topoConf = Utils.findAndReadConfigFile(configPath);
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        topoConf.put(Config.TOPOLOGY_NAME, topologyName);

        switch (Integer.parseInt(topologyNumber)) {
            case 1:
                Helper.runOnClusterAndPrintMetrics(10 * 60, topologyName, topoConf, new FirstTopo().topology());
                break;
            case 2:
                Helper.runOnClusterAndPrintMetrics(10 * 60, topologyName, topoConf, new SecondTopo().topology());
                break;
            case 3:
                Helper.runOnClusterAndPrintMetrics(10 * 60, topologyName, topoConf, new ThirdTopo().topology());
                break;
            case 4:
                Helper.runOnClusterAndPrintMetrics(10 * 60, topologyName, topoConf, new FourthTopo().topology());
                break;
            default:
                break;
        }
    }
}

package storm.v4.output;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class OutputBolt extends BaseRichBolt {
    private Log log = LogFactory.getLog(OutputBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        float pred = tuple.getFloatByField("pred");
        log.info("[ \"url\" : " + url + ", \"pred\" : " + pred + " ]");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

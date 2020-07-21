package storm.input;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class InputSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Printable printable;
    private final String[] url_data = {
            "http://www.sfcrowsnest.com/sfnews2/03_march/news0303_3.shtml",
            "http://artisanearthworks.com/products.php",
            "http://165.227.0.144/bins/rift.sh4",
            "http://www.oldielyrics.com/n/new_york_dolls.html",
            "http://www.localharvest.org/csadrops.jsp?id=16174"
    };
    private int index = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.printable = new Printable();
    }

    @Override
    public void nextTuple() {
        spoutOutputCollector.emit(new Values(printable.convert(url_data[index])));
        index++;
        if (index >= url_data.length) {
            index = 0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url"));
    }
}

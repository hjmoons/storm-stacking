package storm.v4.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class InputSpout extends BaseRichSpout {
    private Log log = LogFactory.getLog(InputSpout.class);
    private SpoutOutputCollector spoutOutputCollector;
    private Random random;
    private final String[] url_data = {
            "http://www.sfcrowsnest.com/sfnews2/03_march/news0303_3.shtml",
            "http://artisanearthworks.com/products.php",
            "http://165.227.0.144/bins/rift.sh4",
            "http://www.oldielyrics.com/n/new_york_dolls.html",
            "http://www.localharvest.org/csadrops.jsp?id=16174",
            "http://pamelamiller.artspan.com",
            "http://www.scottishrugbyradio.com/",
            "https://elntechnology.co.za/wordpress/public/a0xv31q/",
            "http://astrologybybeverlee.homestead.com/",
            "http://47.88.21.111/%20"
    };

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        spoutOutputCollector.emit(new Values(url_data[random.nextInt(10)]));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url"));
    }
}

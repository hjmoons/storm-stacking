package storm.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.metrics.hdrhistogram.HistogramMetric;
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
    private int trans_time;

    // inner class for latency estimation
    private class SentWithTime {
        public final long id;
        public final String url;
        public final long time;

        SentWithTime(long id, String url, long time) {
            this.id = id;
            this.url = url;
            this.time = time;
        }
    }

    // helper's instance
    HistogramMetric _histo;
    long count = 0l;
    long nextTime = 0l;

    public InputSpout(int trans_time) {
        this.trans_time = trans_time;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.random = new Random();

        _histo = new HistogramMetric(3600000000000L, 3);
        topologyContext.registerMetric("comp-lat-histo", _histo, 10); //Update every 10 seconds, so we are not too far behind
    }

    @Override
    public void nextTuple() {
        if (System.currentTimeMillis() >= nextTime) {
            count++;
            String url = url_data[random.nextInt(10)];
            spoutOutputCollector.emit(new Values(count, url), new SentWithTime(count, url, System.nanoTime()));
            nextTime = System.currentTimeMillis() + trans_time;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "url"));
    }

    @Override
    public void ack(Object id) {
        long end = System.nanoTime();
        SentWithTime st = (SentWithTime)id;
        _histo.recordValue(end-st.time);
    }

    @Override
    public void fail(Object id) {
        SentWithTime st = (SentWithTime)id;
        spoutOutputCollector.emit(new Values(st.id, st.url), id);
    }
}

package storm.level1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.util.HashMap;
import java.util.Map;

public class FinalBolt extends BaseRichBolt {
    private Log log = LogFactory.getLog(FinalBolt.class);
    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    private float[][] level0Result = new float[1][3];

    private Map<int[], Float> cnnMap = new HashMap<>();
    private Map<int[], Float> lstmMap = new HashMap<>();
    private Map<int[], Float> gruMap = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.savedModelBundle = SavedModelBundle.load("/home/hjmoon/models/url/","serve");
        this.sess = savedModelBundle.session();
    }

    @Override
    public void execute(Tuple tuple) {
        int[] url = (int[]) tuple.getValueByField("url");

        if(tuple.getSourceComponent().equals("cnn-bolt")) {
            level0Result[0][0] = (float) tuple.getValueByField("cnn");
            if(!cnnMap.containsKey(url)) {
                cnnMap.put(url, level0Result[0][0]);
                return;
            }
            level0Result[0][1] = lstmMap.remove(url);
            level0Result[0][2] = gruMap.remove(url);
        } else if (tuple.getSourceComponent().equals("lstm-bolt")) {
            level0Result[0][1] = (float) tuple.getValueByField("lstm");
            if(!lstmMap.containsKey(url)) {
                lstmMap.put(url, level0Result[0][1]);
                return;
            }
            level0Result[0][0] = cnnMap.remove(url);
            level0Result[0][2] = gruMap.remove(url);
        } else if (tuple.getSourceComponent().equals("gru-bolt")) {
            level0Result[0][2] = (float) tuple.getValueByField("gru");
            if(!gruMap.containsKey(url)) {
                gruMap.put(url, level0Result[0][2]);
                return;
            }
            level0Result[0][0] = cnnMap.remove(url);
            level0Result[0][1] = lstmMap.remove(url);
        }

        Tensor x = Tensor.create(level0Result);
        Tensor result = sess.runner()
                .feed("concatenate_1:0", x)
                .fetch("final_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][1]);

        log.info("############## output data: " + prob);
        outputCollector.emit(new Values(prob));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result"));
    }
}

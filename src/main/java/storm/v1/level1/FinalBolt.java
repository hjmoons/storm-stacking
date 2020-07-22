package storm.v1.level1;

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

    private Map<String, Float> cnnMap = new HashMap<>();
    private Map<String, Float> lstmMap = new HashMap<>();
    private Map<String, Float> gruMap = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.savedModelBundle = SavedModelBundle.load("/home/hjmoon/models/url/","serve");
        this.sess = savedModelBundle.session();
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");

        if(tuple.getSourceComponent().equals("cnn-bolt")) {
            float pred = tuple.getFloatByField("cnn");
            level0Result[0][0] = pred;
            if(!cnnMap.containsKey(url)) {
                cnnMap.put(url, pred);
                return;
            }
            if(lstmMap.containsKey(url)) level0Result[0][1] = lstmMap.remove(url);
            else return;
            if(gruMap.containsKey(url)) level0Result[0][2] = gruMap.remove(url);
            else return;
        } else if (tuple.getSourceComponent().equals("lstm-bolt")) {
            float pred = tuple.getFloatByField("lstm");
            level0Result[0][1] = pred;
            if(!lstmMap.containsKey(url)) {
                lstmMap.put(url, pred);
                return;
            }
            if(cnnMap.containsKey(url)) level0Result[0][0] = cnnMap.remove(url);
            else return;
            if(gruMap.containsKey(url)) level0Result[0][2] = gruMap.remove(url);
            else return;
        } else if (tuple.getSourceComponent().equals("gru-bolt")) {
            float pred = tuple.getFloatByField("gru");
            level0Result[0][2] = pred;
            if(!gruMap.containsKey(url)) {
                gruMap.put(url, pred);
                return;
            }
            if(cnnMap.containsKey(url)) level0Result[0][0] = cnnMap.remove(url);
            else return;
            if(lstmMap.containsKey(url)) level0Result[0][1] = lstmMap.remove(url);
            else return;
        }

        Tensor x = Tensor.create(level0Result);
        Tensor result = sess.runner()
                .feed("final_input/concat:0", x)
                .fetch("final_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] pred = (float[][]) result.copyTo(new float[1][1]);

        outputCollector.emit(new Values(url, pred[0][0]));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "pred"));
    }
}

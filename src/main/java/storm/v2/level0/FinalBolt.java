package storm.v2.level0;

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
import storm.v2.input.Preprocessor;

import java.util.HashMap;
import java.util.Map;

public class FinalBolt extends BaseRichBolt {
    private Log log = LogFactory.getLog(FinalBolt.class);
    private OutputCollector outputCollector;
    private Preprocessor preprocessor;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    private float[][] level0Result = new float[1][3];

    private Map<String, Float> cnnMap = new HashMap<>();
    private Map<String, Float> lstmMap = new HashMap<>();
    private Map<String, Float> gruMap = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.preprocessor = new Preprocessor();
        this.savedModelBundle = SavedModelBundle.load("/home/hjmoon/models/url/","serve");
        this.sess = savedModelBundle.session();
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        int[][] input = preprocessor.convert(url);

        // CNN Model
        Tensor x_1 = Tensor.create(input);
        Tensor result_1 = sess.runner()
                .feed("cnn_input:0", x_1)
                .fetch("cnn_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] cnn_pred = (float[][]) result_1.copyTo(new float[1][1]);

        Tensor x_2 = Tensor.create(input);
        Tensor result_2 = sess.runner()
                .feed("lstm_input:0", x_2)
                .fetch("lstm_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] lstm_pred = (float[][]) result_2.copyTo(new float[1][1]);

        Tensor x_3 = Tensor.create(input);
        Tensor result_3 = sess.runner()
                .feed("gru_input:0", x_3)
                .fetch("gru_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] gru_pred = (float[][]) result_3.copyTo(new float[1][1]);

        level0Result[0][0] = cnn_pred[0][0];
        level0Result[0][1] = lstm_pred[0][0];
        level0Result[0][2] = gru_pred[0][0];

        Tensor x = Tensor.create(level0Result);
        Tensor result = sess.runner()
                .feed("final_input/concat:0", x)
                .fetch("final_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] final_pred = (float[][]) result.copyTo(new float[1][1]);

        outputCollector.emit(new Values(url, final_pred[0][0]));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "pred"));
    }
}

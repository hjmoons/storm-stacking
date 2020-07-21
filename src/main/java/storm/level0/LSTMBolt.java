package storm.level0;

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

import java.util.Map;

public class LSTMBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.savedModelBundle = SavedModelBundle.load("./models/url/","serve");
        this.sess = savedModelBundle.session();
    }

    @Override
    public void execute(Tuple tuple) {
        int[][] data = (int[][]) tuple.getValueByField("url");

        Tensor x = Tensor.create(data);
        Tensor result = sess.runner()
                .feed("ensemble_2_lstm_input:0", x)
                .fetch("ensemble_2_lstm_output/Sigmoid:0")
                .run()
                .get(0);

        float[] prob = (float[]) result.copyTo(new float[1]);

        outputCollector.emit(new Values(prob));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lstm"));
    }
}

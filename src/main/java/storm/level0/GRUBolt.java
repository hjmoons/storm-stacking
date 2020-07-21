package storm.level0;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.util.Map;

import static storm.MainTopology.inferenceModel;

public class GRUBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Session sess;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.sess = inferenceModel.getSession();
    }

    @Override
    public void execute(Tuple tuple) {
        int[][] data = (int[][]) tuple.getValueByField("url");

        Tensor x = Tensor.create(data);
        Tensor result = sess.runner()
                .feed("ensemble_3_gru_input:0", x)
                .fetch("ensemble_3_gru_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][1]);

        outputCollector.emit(new Values(prob));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("gru"));
    }
}

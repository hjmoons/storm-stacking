package storm;

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

public class Model_Final_Bolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Session sess;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.sess = inferenceModel.getSession();
    }

    @Override
    public void execute(Tuple tuple) {


        float[][] data = new float[1][80];
        Tensor x = Tensor.create(data);
        Tensor result = sess.runner()
                .feed("concatenate_1:0", x)
                .fetch("final_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][1]);

        outputCollector.emit(new Values(prob));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result"));
    }
}

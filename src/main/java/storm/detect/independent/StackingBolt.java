package storm.detect.independent;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.springframework.core.io.ClassPathResource;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

public class StackingBolt extends BaseRichBolt {
    class PredictionTriplet {
        float cnn = -1;
        float lstm = -1;
        float gru = -1;
    }
    private Log log = LogFactory.getLog(StackingBolt.class);
    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    private Map<Long, PredictionTriplet> preds = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        File directory = new File("variables");
        if (! directory.exists()){
            directory.mkdir();
        }

        ClassPathResource model = new ClassPathResource("saved_model.pb");
        ClassPathResource v1 = new ClassPathResource("variables/variables.data-00000-of-00001");
        ClassPathResource v2 = new ClassPathResource("variables/variables.index");

        try {
            File modelFile = new File("./saved_model.pb");
            File v1File = new File("./variables/variables.data-00000-of-00001");
            File v2File = new File("./variables/variables.index");
            IOUtils.copy(model.getInputStream(),new FileOutputStream(modelFile));
            IOUtils.copy(v1.getInputStream(),new FileOutputStream(v1File));
            IOUtils.copy(v2.getInputStream(),new FileOutputStream(v2File));

            this.savedModelBundle = SavedModelBundle.load("./", "serve");
            this.sess = savedModelBundle.session();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        long id = tuple.getLongByField("id");

        PredictionTriplet triplet;
        if (preds.containsKey(id)) {
            triplet = preds.get(id);
        } else {
            triplet = new PredictionTriplet();
            preds.put(id, triplet);
        }

        switch (tuple.getSourceComponent()) {
            case "cnn-bolt":
                triplet.cnn = tuple.getFloatByField("cnn");
                break;
            case "lstm-bolt":
                triplet.lstm = tuple.getFloatByField("lstm");
                break;
            case "gru-bolt":
                triplet.gru = tuple.getFloatByField("gru");
                break;
        }

        if (triplet.cnn == -1 || triplet.lstm == -1 || triplet.gru == -1)
            return;

        float[][] level0Result = new float[][]{{triplet.cnn, triplet.lstm, triplet.gru}};

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

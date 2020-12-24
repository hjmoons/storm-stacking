package storm.detect.semi_integrated;

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
import storm.input.Preprocessor;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

public class BaseModelBolt extends BaseRichBolt {
    private Log log = LogFactory.getLog(BaseModelBolt.class);
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

        outputCollector.emit(new Values(url, cnn_pred[0][0], lstm_pred[0][0], gru_pred[0][0]));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "cnn", "lstm", "gru"));
    }
}

package storm;

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
import java.util.Map;

/**
 * 악성 URL 탐지 모델에 사용된 LSTM 모델을 실행하는 볼트 클래스
 */
public class LSTMBolt extends BaseRichBolt {
    private Log log = LogFactory.getLog(LSTMBolt.class);
    private OutputCollector outputCollector;
    private Preprocessor preprocessor;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.preprocessor = new Preprocessor();

        File directory = new File("variables");
        if (! directory.exists()){
            directory.mkdir();
        }

        // resource 폴더에 있는 모델 탐색
        ClassPathResource model = new ClassPathResource("saved_model.pb");
        ClassPathResource v1 = new ClassPathResource("variables/variables.data-00000-of-00001");
        ClassPathResource v2 = new ClassPathResource("variables/variables.index");

        try {
            // resource 폴더에 있는 모델을 스톰이 실행되는 서버에 다운로드
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
        String url = tuple.getStringByField("url"); // 입력된 URL 데이터
        float cnn = tuple.getFloatByField("cnn");   // CNN의 예측 결과 데이터

        int[][] input = preprocessor.convert(url);  // URL 데이터 전처리

        // 저장된 모델 실행
        Tensor x = Tensor.create(input);
        Tensor result = sess.runner()
                .feed("lstm_input:0", x)
                .fetch("lstm_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] pred = (float[][]) result.copyTo(new float[1][1]);    // 결과 데이터

        outputCollector.emit(new Values(url, cnn, pred[0][0]));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "cnn", "lstm"));
    }
}

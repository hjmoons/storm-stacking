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
import java.util.HashMap;
import java.util.Map;

/**
 * 악성 URL 탐지 모델의 Final 부분을 실행하는 볼트 클래스
 */
public class FianlBolt extends BaseRichBolt {
    private Log log = LogFactory.getLog(FianlBolt.class);
    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    private float[][] level0Result = new float[1][3];   // CNN, LSTM, GRU 볼트로 부터 전송된 예측 결과 데이터

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

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

            // 저장된 모델 불러오기
            this.savedModelBundle = SavedModelBundle.load("./", "serve");
            this.sess = savedModelBundle.session();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url"); // 입력된 URL 데이터

        level0Result[0][0] = tuple.getFloatByField("cnn");
        level0Result[0][1] = tuple.getFloatByField("lstm");
        level0Result[0][2] = tuple.getFloatByField("gru");

        // 저장된 모델 실행
        Tensor x = Tensor.create(level0Result);
        Tensor result = sess.runner()
                .feed("final_input/concat:0", x)
                .fetch("final_output/Sigmoid:0")
                .run()
                .get(0);

        float[][] pred = (float[][]) result.copyTo(new float[1][1]);    // 결과 데이터

        String output = url + "," + String.valueOf(pred[0][0]); // Kafka 볼트에 출력될 결과

        outputCollector.emit(new Values(output));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}

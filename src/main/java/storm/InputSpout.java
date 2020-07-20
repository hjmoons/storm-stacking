package storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.util.StringTokenizer;

public class InputSpout extends BaseRichSpout {
    private String data_path = "C:\\Users\\HyoJong\\VSProjects\\Staking\\data\\url_label.csv";

    private Printable printable;
    private OutputCollector outputCollector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = outputCollector;
        this.printable = new Printable();
    }

    @Override
    public void nextTuple() {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(data_path));
            String line = null;
            while((line = bufferedReader.readLine()) != null) {
                String []tokens = line.split(",");
                int[][] url = printable.convert(tokens[0]);
                outputCollector.emit(new Values(url));
            }
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data"));
    }
}

package storm;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;
import java.util.UUID;

public class StackingTopology {

    private final int NUM_KAFKASPOUT = 1;
    private final int NUM_CNNBOLT = 1;
    private final int NUM_LSTMBOLT = 1;
    private final int NUM_GRUBOLT = 1;
    private final int NUM_FINALBOLT = 1;
    private final int NUM_KAFKABOLT = 1;

    public static void main(String[] args) throws Exception {
        // Kafka의 Input, Output 토픽 입력 확인
        if (args.length < 2) {
            System.out.println("No arguments InputTopic, OutputTopic.");
            System.exit(0);
        }

        String inputTopic = args[0];
        String outputTopic = args[1];

        // 실행시간
        int numSecs = 300;
        if (args != null && args.length > 2) {
            numSecs = Integer.valueOf(args[2]);
        }

        // Zookeeper 서버 주소
        String zkHosts = "MN:2181,SN01:2181,SN02:2181,SN03:2181";
        if (args != null && args.length > 3) {
            zkHosts = args[3];
        }

        // Kafka 서버 주소
        String bootstrap = "MN:9092,SN01:9092,SN02:9092,SN03:9092";
        if (args != null && args.length > 4) {
            bootstrap = args[4];
        }

        // 토폴로지 이름
        String topologyName = "Storm-Stacking";
        if (args != null && args.length > 5) {
            topologyName = args[5];
        }

        // 토폴로지 실행
        Config config = new Config();
        config.setNumWorkers(3);
        Cluster cluster = new Cluster(config);
        try {
            cluster.submitTopology(topologyName, config, new StackingTopology().topology(zkHosts, bootstrap, inputTopic, outputTopic));
            Thread.sleep(numSecs * 1000);
        } finally {
            cluster.killTopology(topologyName);
        }

        System.exit(0);
    }

    /* 토폴로지 구성 함수 */
    public StormTopology topology(String zkhosts, String bootstrap, String inputTopic, String outputTopic) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig(zkhosts, inputTopic)), NUM_KAFKASPOUT);
        topologyBuilder.setBolt("cnn-bolt", new CNNBolt(), NUM_CNNBOLT).shuffleGrouping("kafka-spout");
        topologyBuilder.setBolt("lstm-bolt", new LSTMBolt(), NUM_LSTMBOLT).shuffleGrouping("cnn-bolt");
        topologyBuilder.setBolt("gru-bolt", new GRUBolt(), NUM_GRUBOLT).shuffleGrouping("lstm-bolt");
        topologyBuilder.setBolt("final-bolt", new FianlBolt(), NUM_FINALBOLT).shuffleGrouping("gru-bolt");
        topologyBuilder.setBolt("kafka-bolt",new KafkaBolt().withProducerProperties(kafkaBoltConfig(bootstrap))
                .withTopicSelector(new DefaultTopicSelector(outputTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper()), NUM_KAFKABOLT).shuffleGrouping("final-bolt");

        return topologyBuilder.createTopology();
    }

    /* Kafka Spout Configuration */
    public SpoutConfig kafkaSpoutConfig(String zkhosts, String inputTopic) {
        BrokerHosts brokerHosts = new ZkHosts(zkhosts);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, inputTopic, "/" + inputTopic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();    // To pull the latest data in topic
        kafkaSpoutConfig.ignoreZkOffsets = true;
        kafkaSpoutConfig.maxOffsetBehind = 0;

        return kafkaSpoutConfig;
    }

    /* KafkaBolt Configuration */
    public Properties kafkaBoltConfig(String bootstrap) {
        Properties kafkaBoltConfig = new Properties();
        kafkaBoltConfig.put("metadata.broker.list", bootstrap);
        kafkaBoltConfig.put("bootstrap.servers", bootstrap);
        kafkaBoltConfig.put("acks", "1");
        kafkaBoltConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaBoltConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return kafkaBoltConfig;
    }

}

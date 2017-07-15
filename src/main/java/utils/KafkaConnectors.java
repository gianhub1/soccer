package utils;


import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import schema.SensorDataSchema;

import java.util.Properties;

public class KafkaConnectors {

    public static final FlinkKafkaConsumer010<String> kafkaConsumer(String topic, String zookeeper, String kafka) {

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect",zookeeper);
        kafkaProps.setProperty("bootstrap.servers",kafka);
        kafkaProps.setProperty("group.id", "myGroup");

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(topic,
                new SensorDataSchema(),
                kafkaProps);
        return consumer;
    }

}
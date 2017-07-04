package kafka;


import model.SensorData;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import schema.SensorDataSchema;
import utils.JSONFormatter;

import java.util.Properties;

/**
 * Created by marco on 24/06/17.
 */
public class KafkaConnectors {

    public static final FlinkKafkaConsumer010<SensorData> kafkaConsumer(String topic, String zookeeper, String kafka) {

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect",zookeeper);
        kafkaProps.setProperty("bootstrap.servers",kafka);
        //kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("group.id", "myGroup");

        FlinkKafkaConsumer010<SensorData> consumer = new FlinkKafkaConsumer010<>(topic,
                new SensorDataSchema(),
                kafkaProps);
        return consumer;
    }


    public static final void kafkaProducer(String topic, String key, String kafka,SensorData sensorData) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>(topic,key, JSONFormatter.serialize(sensorData)));
        producer.close();
    }

}



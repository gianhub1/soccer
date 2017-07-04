package core;

import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import kafka.KafkaConnectors;
import model.SensorData;
import operator.fold.AverageFF;
import operator.key.SensorKey;
import operator.window.AverageWF;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import time.SensorDataExtractor;


/**
 * Created by marco on 24/06/17.
 */

/**
 *
 * Goal:analyze the running performance of each of the players currently participating in the game
 * •  Output:thea ggregate running statistics
 * ts_start, ts_stop, player_id, total distance, avg speed
 * •  The aggregate running statistics mustbe calculated using three different time windows:
 *  –  1 minute
 *  –  5 minutes
 *  –  entire match
 *
 */
public class QueryOne {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment();

        FlinkKafkaConsumer010<SensorData> kafkaConsumer = KafkaConnectors.kafkaConsumer(AppConfiguration.TOPIC,AppConfiguration.CONSUMER_ZOOKEEPER_HOST,
                AppConfiguration.CONSUMER_KAFKA_BROKER);

        FlinkKafkaConsumerBase<SensorData> kafkaConsumerTS = kafkaConsumer.assignTimestampsAndWatermarks(new SensorDataExtractor());

        DataStream<SensorData> sensorDataStream = env.addSource(kafkaConsumerTS);

        //DataStream<SensorData> filteredSDS = sensorDataStream.filter(new NoBallsAndRefsFilter());

        WindowedStream windowedSDS = sensorDataStream.keyBy(new SensorKey()).timeWindow(Time.minutes(1));

        SingleOutputStreamOperator queryOneOutput = windowedSDS.fold(new Tuple5<>(null,0L,new Double(0),0L,0L), new AverageFF(),new AverageWF());

        queryOneOutput.print();

        env.execute("SoccerQueryOne");

    }
}

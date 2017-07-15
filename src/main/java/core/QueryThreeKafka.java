package core;

import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import model.SensorData;
import operator.filter.NoBallsAndRefsFilter;
import operator.flatmap.StringMapperFD;
import operator.fold.HeatMapAggregateFF;
import operator.fold.HeatMapFF;
import operator.key.HeatMapKey;
import operator.key.SensorSid;
import operator.window.HeatMapAggregateWF;
import operator.window.HeatMapWF;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import time.SensorDataExtractor;
import utils.KafkaConnectors;

/**
 * Created by marco on 15/07/17.
 */
public class QueryThreeKafka {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment(args);

        System.exit(0);
        FlinkKafkaConsumer010<String> kafkaConsumer = KafkaConnectors.kafkaConsumer(AppConfiguration.TOPIC,AppConfiguration.CONSUMER_ZOOKEEPER_HOST,
                AppConfiguration.CONSUMER_KAFKA_BROKER);

        DataStream<SensorData> sensorDataStream = env
                .addSource(kafkaConsumer)
                .setParallelism(8)
                .flatMap(new StringMapperFD())
                .filter(new NoBallsAndRefsFilter());

        /**
         * Minute HeatMap by leg
         */
        WindowedStream windowedSDS = sensorDataStream
                .assignTimestampsAndWatermarks(new SensorDataExtractor())
                .keyBy(new SensorSid())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator sidOutput = windowedSDS.fold(new Tuple4<>(0L,null, null,0L), new HeatMapFF(),new HeatMapWF());

        /**
         * Minute HeatMap by player
         */
        WindowedStream playerMinuteHeatMapWindow = sidOutput
                .keyBy(new HeatMapKey())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator playerMinuteHeatMapOutput = playerMinuteHeatMapWindow.fold(new Tuple4<>(0L,null, null,null), new HeatMapAggregateFF(),new HeatMapAggregateWF(true));
        //playerMinuteHeatMapOutput.print();

        /**
         * Match HeatMap by player
         */
        WindowedStream playerMatchHeatMapWindow = sidOutput
                .keyBy(new HeatMapKey())
                .timeWindow(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET))
                .allowedLateness(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET - 1));
        SingleOutputStreamOperator playerMatchHeatMapOutput = playerMatchHeatMapWindow.fold(new Tuple4<>(0L,null, null,null), new HeatMapAggregateFF(),new HeatMapAggregateWF(false));
        //playerMatchHeatMapOutput.print();

        env.execute("SoccerQueryThreeKafka");

    }
}


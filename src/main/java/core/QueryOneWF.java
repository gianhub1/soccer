package core;

import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import model.SensorData;
import operator.flatmap.StringMapper;
import operator.key.SensorKey;
import operator.key.SensorSid;
import operator.window.compute.AverageWF;
import operator.window.compute.TupleAverageWF;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import time.SensorDataExtractor;
import time.TupleExtractor;


public class QueryOneWF {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment(args);


        DataStream<SensorData> fileStream = env.readTextFile(AppConfiguration.FILTERED_DATASET_FILE).setParallelism(1)
                .flatMap(new StringMapper());
        /**
         * Average speed and total distance by sid in 1 minute
         */
        WindowedStream windowedSDS = fileStream
                .assignTimestampsAndWatermarks(new SensorDataExtractor())
                .keyBy(new SensorSid())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator sidOutput = windowedSDS
                .apply(new AverageWF());
        /**
         * Average speed and total distance by player in 1 minute
         */
        WindowedStream minutePlayerStream = sidOutput
                .keyBy(new SensorKey())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator minutePlayerOutput = minutePlayerStream
                .apply(new TupleAverageWF(true));
        //minutePlayerOutput.print();

        /**
         * Average speed and total distance by player in 5 minute
         */
        WindowedStream fiveMinutePlayerStream = minutePlayerOutput
                .keyBy(new SensorKey())
                .timeWindow(Time.minutes(5));
        SingleOutputStreamOperator fiveMinutePlayerOutput = fiveMinutePlayerStream
                .apply(new TupleAverageWF(false));
        //fiveMinutePlayerOutput.printToErr();

        /**
         * Average speed and total distance by player in all match
         */
        WindowedStream allMatchPlayerStream = fiveMinutePlayerOutput
                .assignTimestampsAndWatermarks(new TupleExtractor())
                .keyBy(new SensorKey())
                .timeWindow(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET))
                .allowedLateness(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET - 1));
        SingleOutputStreamOperator allMatchPlayerOutput = allMatchPlayerStream
                .apply(new TupleAverageWF(false));
        //allMatchPlayerOutput.printToErr();

        env.execute("SoccerQueryOneWF");

    }
}
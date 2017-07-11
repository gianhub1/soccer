package core;

import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import model.SensorData;
import operator.filter.NoBallsAndRefsFilter;
import operator.flatmap.StringMapperFD;
import operator.fold.AggregateFF;
import operator.fold.AverageFF;
import operator.fold.RankFF;
import operator.key.SensorKey;
import operator.key.SensorSid;
import operator.window.PlayerWF;
import operator.window.SensorWF;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import time.SensorDataExtractor;
import time.TupleExtractor;

/**
 * Created by marco on 07/07/17.
 */
public class QueryTwoFD {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment(args);


        DataStream<SensorData> fileStream = env
                .readTextFile(AppConfiguration.FULL_DATASET_FILE).setParallelism(1)
                .flatMap(new StringMapperFD())
                .filter(new NoBallsAndRefsFilter());
        /**
         * Average speed by sid in 1 minute
         */
        WindowedStream windowedSDS = fileStream
                .assignTimestampsAndWatermarks(new SensorDataExtractor())
                .keyBy(new SensorSid())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator sidOutput = windowedSDS
                .fold(new Tuple4<>(0L,0L, null,0L), new AverageFF(),new SensorWF());

        /**
         * Average speed by player in 1 minute
         */
        WindowedStream minutePlayerStream = sidOutput
                .keyBy(new SensorKey())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator minutePlayerOutput = minutePlayerStream
                .fold(new Tuple6<>(0L,0L,"", 0d, 0d,0L), new AggregateFF(true), new PlayerWF());

        /**
         * Top 5 rank in 1 minute
         */
        AllWindowedStream rankMinuteWindow = minutePlayerOutput
                .assignTimestampsAndWatermarks(new TupleExtractor())
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)));
        SingleOutputStreamOperator rankMinuteOutput = rankMinuteWindow
                .fold(new Tuple3<>(0L, 0L, null), new RankFF()).setParallelism(1);
        //rankMinuteOutput.print();

        /**
         * Top 5 rank in 5 minutes
         */
        AllWindowedStream rankFiveMinuteWindow = minutePlayerOutput
                .assignTimestampsAndWatermarks(new TupleExtractor())
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)));
        SingleOutputStreamOperator rankFiveMinuteOutput = rankFiveMinuteWindow
                .fold(new Tuple3<>(0L, 0L, null), new RankFF()).setParallelism(1);
        //rankFiveMinuteOutput.print();

        /**
         * Top 5 rank in all match
         */
        AllWindowedStream rankMatchMinuteWindow = minutePlayerOutput
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET )
                        ,Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET -1)));
        SingleOutputStreamOperator rankMatchOutput = rankMatchMinuteWindow
                .fold(new Tuple3<>(0L, 0L, null), new RankFF()).setParallelism(1);
        //rankMatchOutput.print();


        env.execute("SoccerQueryTwoFD");

    }
}

package core;



import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import model.SensorData;
import operator.filter.NoBallsAndRefsFilter;
import operator.flatmap.StringMapperFD;
import operator.fold.AggregateFF;
import operator.fold.AverageFF;
import operator.key.SensorKey;
import operator.key.SensorSid;
import operator.window.PlayerWF;
import operator.window.SensorWF;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import time.SensorDataExtractor;
import time.TupleExtractor;


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
public class QueryOneFD {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment(args);


        DataStream<SensorData> fileStream = env
                .readTextFile(AppConfiguration.FULL_DATASET_FILE).setParallelism(1)
                .flatMap(new StringMapperFD())
                .filter(new NoBallsAndRefsFilter());
        /**
         * Average speed and total distance by sid in 1 minute
         */
        WindowedStream windowedSDS = fileStream
                .assignTimestampsAndWatermarks(new SensorDataExtractor())
                .keyBy(new SensorSid())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator sidOutput = windowedSDS
                .fold(new Tuple4<>(0L,0L, null,0L), new AverageFF(),new SensorWF());

        /**
         * Average speed and total distance by player in 1 minute
         */
        WindowedStream minutePlayerStream = sidOutput
                .keyBy(new SensorKey())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator minutePlayerOutput = minutePlayerStream
                .fold(new Tuple6<>(0L,0L,"", 0d, 0d,0L), new AggregateFF(true), new PlayerWF());
        minutePlayerOutput.writeAsText(AppConfiguration.QUERY_ONE_OUTPUT + "_1M").setParallelism(1);
        //minutePlayerOutput.print();

        /**
         * Average speed and total distance by player in 5 minute
         */
        WindowedStream fiveMinutePlayerStream = minutePlayerOutput
                .keyBy(new SensorKey())
                .timeWindow(Time.minutes(5));
        SingleOutputStreamOperator fiveMinutePlayerOutput = fiveMinutePlayerStream
                .fold(new Tuple6<>(0L,0L,"", 0d, 0d,0L), new AggregateFF(false), new PlayerWF());
        //fiveMinutePlayerOutput.print();
        fiveMinutePlayerOutput.writeAsText(AppConfiguration.QUERY_ONE_OUTPUT + "_5M").setParallelism(1);


        /**
         * Average speed and total distance by player in all match
         */
        WindowedStream allMatchPlayerStream = fiveMinutePlayerOutput
                .assignTimestampsAndWatermarks(new TupleExtractor())
                .keyBy(new SensorKey())
                .timeWindow(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET))
                .allowedLateness(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET - 1));
        SingleOutputStreamOperator allMatchPlayerOutput = allMatchPlayerStream
                .fold(new Tuple6<>(0L,0L,"", 0d, 0d,0L), new AggregateFF(false), new PlayerWF());
        //allMatchPlayerOutput.print();
        allMatchPlayerOutput.writeAsText(AppConfiguration.QUERY_ONE_OUTPUT + "_AM").setParallelism(1);


        env.execute("SoccerQueryOneFD");

    }
}

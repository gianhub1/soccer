package core;

import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import model.SensorData;
import operator.flatmap.StringMapper;
import operator.fold.AggregateFF;
import operator.fold.AverageFF;
import operator.key.SensorKey;
import operator.key.SensorSid;
import operator.window.PlayerWF;
import operator.window.SensorWF;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
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


        DataStream<SensorData> fileStream = env.readTextFile(AppConfiguration.OUTPUT_FILE).setParallelism(1).flatMap(new StringMapper());

        /**
         * Average speed and total distance by sid in 1 minute
         */
        WindowedStream windowedSDS = fileStream.assignTimestampsAndWatermarks(new SensorDataExtractor()).keyBy(new SensorSid()).timeWindow(Time.minutes(1));
        SingleOutputStreamOperator sidOutput = windowedSDS.fold(new Tuple5<>(0L,0L, null, 0d,0L), new AverageFF(true),new SensorWF());

        //sidOutput.print();
        /**
         * Average speed and total distance by player in 1 minute
         */
        WindowedStream minutePlayerStream = sidOutput.keyBy(new SensorKey()).timeWindow(Time.minutes(1));
        SingleOutputStreamOperator minutePlayerOutput = minutePlayerStream.fold(new Tuple6<>(0L,0L,"", 0d, 0d,0L), new AggregateFF(), new PlayerWF());
        //minutePlayerOutput.print();

        /**
         * Average speed and total distance by player in 5 minute
         */
        WindowedStream fiveMinutePlayerStream = minutePlayerOutput.keyBy(new SensorKey()).timeWindow(Time.minutes(5));
        SingleOutputStreamOperator fiveMinutePlayerOutput = fiveMinutePlayerStream.fold(new Tuple6<>(0L,0L,"", 0d, 0d,0L), new AggregateFF(), new PlayerWF());
        //fiveMinutePlayerOutput.print();

        /**
         * Average speed and total distance by player in all match
         */
        WindowedStream allMatchPlayerStream = fiveMinutePlayerOutput.keyBy(new SensorKey()).timeWindow(Time.minutes((long) Math.ceil((((AppConfiguration.TS_MATCH_STOP-AppConfiguration.TS_MATCH_START)/1000000000)/1000)/60)));
        SingleOutputStreamOperator allMatchPlayerOutput = allMatchPlayerStream.fold(new Tuple6<>(0L,0L,"", 0d, 0d,0L), new AggregateFF(), new PlayerWF());
        allMatchPlayerOutput.print();

        env.execute("SoccerQueryOne");

    }
}

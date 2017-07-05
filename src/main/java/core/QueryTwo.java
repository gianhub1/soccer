package core;

import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import model.SensorData;
import operator.flatmap.StringMapper;
import operator.fold.AverageSpeedFF;
import operator.key.SensorKey;
import operator.key.SensorSid;
import operator.reduce.ReducePlayer;
import operator.reduce.ReduceSid;
import operator.window.PlayerWF;
import operator.window.SensorWF;
import org.apache.flink.api.java.tuple.Tuple5;
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
 * Goal: analyze the running performance of every player participating in the game
 * •  Output2: top-5 players by average speed
 ts_start, ts_stop, player_id_1, avg_speed_1, player_id_2, avg_speed_2, player_id_3, avg_speed_3, ...
 * •  The aggregate running statistics must be calculated using three different time windows:
 *  –  1 minute
 *  –  5 minutes
 *  –  entire match
 *
 */
public class QueryTwo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment();


        DataStream<SensorData> fileStream = env.readTextFile(AppConfiguration.OUTPUT_FILE+"_new").flatMap(new StringMapper()).assignTimestampsAndWatermarks(new SensorDataExtractor()).setParallelism(1);

        /**
         * Average speed and total distance by sid in 1 minute
         */
        WindowedStream windowedSDS = fileStream.keyBy(new SensorSid()).timeWindow(Time.minutes(1));
        SingleOutputStreamOperator sidOutput = windowedSDS.fold(new Tuple5<>(null,0L,new Double(0),0L,0L), new AverageSpeedFF(),new SensorWF());

        /**
         * Average speed and total distance by player in 1 minute
         */
        WindowedStream minutePlayerStream = sidOutput.keyBy(new SensorKey()).timeWindow(Time.minutes(1));
        SingleOutputStreamOperator playerMinuteOutput = minutePlayerStream.reduce(new ReduceSid(), new PlayerWF());

        //playerMinuteOutput.print();

        /**
         * Average speed and total distance by player in 5 minute
         */
        WindowedStream fiveMinutePlayerStream = playerMinuteOutput.keyBy(new SensorKey()).timeWindow(Time.minutes(5));
        SingleOutputStreamOperator fiveMinutePlayerOutput = fiveMinutePlayerStream.reduce(new ReducePlayer(), new PlayerWF());

        //fiveMinutePlayerOutput.print();

        /**
         * Average speed and total distance by player in all match
         */
        WindowedStream allMatchPlayerStream = fiveMinutePlayerOutput.keyBy(new SensorKey()).timeWindow(Time.minutes((long) Math.ceil((((AppConfiguration.TS_MATCH_STOP-AppConfiguration.TS_MATCH_START)/1000000000)/1000)/60)));
        SingleOutputStreamOperator allMatchPlayerOutput = allMatchPlayerStream.reduce(new ReducePlayer(), new PlayerWF());

        //allMatchPlayerOutput.print();

        env.execute("SoccerQueryTwo");

    }
}

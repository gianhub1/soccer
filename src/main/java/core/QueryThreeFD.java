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
import time.SensorDataExtractor;


public class QueryThreeFD {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment(args);

        DataStream<SensorData> fileStream = env
                .readTextFile(AppConfiguration.FULL_DATASET_FILE).setParallelism(1)
                .flatMap(new StringMapperFD())
                .filter(new NoBallsAndRefsFilter());

        /**
         * Minute HeatMap by leg
         */
        WindowedStream windowedSDS = fileStream
                .assignTimestampsAndWatermarks(new SensorDataExtractor())
                .keyBy(new SensorSid())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator sidOutput = windowedSDS
                .fold(new Tuple4<>(0L,null, null,0L), new HeatMapFF(),new HeatMapWF());

        /**
         * Minute HeatMap by player
         */
        WindowedStream playerMinuteHeatMapWindow = sidOutput
                .keyBy(new HeatMapKey())
                .timeWindow(Time.minutes(1));
        SingleOutputStreamOperator playerMinuteHeatMapOutput = playerMinuteHeatMapWindow
                .fold(new Tuple4<>(0L,null, null,null), new HeatMapAggregateFF(),new HeatMapAggregateWF());
        //playerMinuteHeatMapOutput

        /**
         * Match HeatMap by player
         */
        WindowedStream playerMatchHeatMapWindow = sidOutput
                .keyBy(new HeatMapKey())
                .timeWindow(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET))
                .allowedLateness(Time.minutes(AppConfiguration.MATCH_DURATION + AppConfiguration.OFFSET - 1));
        SingleOutputStreamOperator playerMatchHeatMapOutput = playerMatchHeatMapWindow
                .fold(new Tuple4<>(0L,null, null,null), new HeatMapAggregateFF(),new HeatMapAggregateWF());
        //playerMatchHeatMapOutput.print();

        env.execute("SoccerQueryThreeFD");

    }
}

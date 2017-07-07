package core;

import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import model.SensorData;
import operator.flatmap.StringMapper;
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

/**
 * Created by marco on 24/06/17.
 */
public class QueryThree {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment();

        DataStream<SensorData> fileStream = env.readTextFile(AppConfiguration.OUTPUT_FILE).setParallelism(1).flatMap(new StringMapper());

        WindowedStream windowedSDS = fileStream.assignTimestampsAndWatermarks(new SensorDataExtractor()).keyBy(new SensorSid()).timeWindow(Time.minutes(1));
        SingleOutputStreamOperator sidOutput = windowedSDS.fold(new Tuple4<>(0L,null, null,0L), new HeatMapFF(),new HeatMapWF());

        WindowedStream playerHeatMapWindow = sidOutput.keyBy(new HeatMapKey()).timeWindow(Time.minutes(1));
        SingleOutputStreamOperator playerHeatMapOutput = playerHeatMapWindow.fold(new Tuple4<>(0L,null, null,null), new HeatMapAggregateFF(),new HeatMapAggregateWF());

        playerHeatMapOutput.writeAsText("/Users/marco/Desktop/queryThree").setParallelism(1);

        env.execute("SoccerQueryThree");

    }
}

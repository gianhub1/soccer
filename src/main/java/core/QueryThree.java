package core;

import configuration.AppConfiguration;
import configuration.FlinkEnvConfig;
import model.SensorData;
import operator.flatmap.StringMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by marco on 24/06/17.
 */
public class QueryThree {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = FlinkEnvConfig.setupExecutionEnvironment();

        DataStream<SensorData> fileStream = env.readTextFile(AppConfiguration.OUTPUT_FILE).setParallelism(1).flatMap(new StringMapper());

        env.execute("SoccerQueryThree");

    }
}

package configuration;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.DatasetMap;

public class FlinkEnvConfig {

    public static final StreamExecutionEnvironment setupExecutionEnvironment(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DatasetMap.initMap();
        if (args != null && args.length>0 && args[0]!= null ){
            AppConfiguration.FILTERED_DATASET_FILE = args[0];
            AppConfiguration.FULL_DATASET_FILE = args[0];
        }
        if (args != null && args.length>1 && args[1]!= null ){
            AppConfiguration.QUERY_ONE_OUTPUT = args[1];
            AppConfiguration.QUERY_TWO_OUTPUT = args[1];
            AppConfiguration.QUERY_THREE_OUTPUT = args[1];
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(AppConfiguration.WATERMARK_INTERVAL);

        return env;
    }
}


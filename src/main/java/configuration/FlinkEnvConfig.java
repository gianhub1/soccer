package configuration;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.DatasetMap;

/**
 * Created by marco on 24/06/17.
 */


public class FlinkEnvConfig {

    public static final StreamExecutionEnvironment setupExecutionEnvironment(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DatasetMap.initMap();
        if (args != null && args.length>0 && args[0]!= null){
            AppConfiguration.FILTERED_DATASET_FILE = args[0];
            AppConfiguration.FULL_DATASET_FILE = args[0];
        }
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(AppConfiguration.WATERMARK_INTERVAL);

        return env;
    }
}


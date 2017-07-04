package configuration;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.DatasetMap;

/**
 * Created by marco on 24/06/17.
 */


public class FlinkEnvConfig {

    public static final StreamExecutionEnvironment setupExecutionEnvironment() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DatasetMap.initMap();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(AppConfiguration.WATERMARK_INTERVAL);

        return env;
    }
}


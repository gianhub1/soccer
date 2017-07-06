package time;

import model.SensorData;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Created by marco on 24/06/17.
 */
public class SensorDataExtractor extends AscendingTimestampExtractor<SensorData> {

    @Override
    public long extractAscendingTimestamp(SensorData sensorData) {
        return sensorData.getTs();
    }

}

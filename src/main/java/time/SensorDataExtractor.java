package time;

import model.SensorData;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Created by marco on 24/06/17.
 */
public class SensorDataExtractor extends AscendingTimestampExtractor<SensorData> {

/*    private static final long MAX_EVENT_DELAY = 1;

    public SensorDataExtractor() {
        super(Time.milliseconds(MAX_EVENT_DELAY));
    }*/

    @Override
    public long extractAscendingTimestamp(SensorData sensorData) {
        return sensorData.getTs();
    }

/*    @Override
    public long extractTimestamp(SensorData sensorData) {
        if (sensorData != null){
            return sensorData.getTs();
        }
        else
            return 0;
    }*/
}

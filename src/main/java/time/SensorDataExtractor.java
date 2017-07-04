package time;

import model.SensorData;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by marco on 24/06/17.
 */
public class SensorDataExtractor extends BoundedOutOfOrdernessTimestampExtractor<SensorData> {

    private static final long MAX_EVENT_DELAY = 1;

    public SensorDataExtractor() {
        super(Time.seconds(MAX_EVENT_DELAY));
    }

    @Override
    public long extractTimestamp(SensorData sensorData) {
        if (sensorData != null){
            return sensorData.getTs();
        }
        else
            return 0;
    }
}

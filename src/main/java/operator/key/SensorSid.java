package operator.key;

import model.SensorData;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Created by marco on 24/06/17.
 */
public class SensorSid implements KeySelector<SensorData, String>{

    @Override
    public String getKey(SensorData sensorData) throws Exception {
        return String.valueOf(sensorData.getSid());
    }
}

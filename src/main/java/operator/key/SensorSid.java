package operator.key;

import model.SensorData;
import org.apache.flink.api.java.functions.KeySelector;


public class SensorSid implements KeySelector<SensorData, String>{

    @Override
    public String getKey(SensorData sensorData) throws Exception {
        return String.valueOf(sensorData.getSid());
    }
}

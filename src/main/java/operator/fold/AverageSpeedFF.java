package operator.fold;

import model.SensorData;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by marco on 05/07/17.
 */
public class AverageSpeedFF implements FoldFunction<SensorData, Tuple4<SensorData, Long,Long,Long>> {

    @Override
    public Tuple4<SensorData, Long, Long,Long> fold(Tuple4<SensorData, Long, Long,Long> in, SensorData sensorData) throws Exception {
        if(in.f0 != null) {
            sensorData.setV(in.f0.getV() + (sensorData.getV() - in.f0.getV()) / (in.f1 + 1));
            return new Tuple4<>(sensorData, in.f1 + 1,in.f3,sensorData.getTs());
        }
        else
            return new Tuple4<>(sensorData, (long)1,sensorData.getTs(),sensorData.getTs());

    }


}
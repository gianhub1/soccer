package operator.fold;

import model.SensorData;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by marco on 04/07/17.
 */
public class AverageFF implements FoldFunction<SensorData, Tuple4<Long, Long,SensorData,Long>> {



    @Override
    public Tuple4<Long, Long,SensorData,Long> fold(Tuple4<Long, Long,SensorData,Long> in, SensorData sensorData) throws Exception {
        if(in.f2 != null) {
            sensorData.setV(in.f2.getV() + (sensorData.getV() - in.f2.getV()) / (in.f3 + 1));
            long start_timestamp = 0;
            long stop_timestamp = 0;
            if (in.f0 >= sensorData.getTs())
                start_timestamp = sensorData.getTs();
            else
                start_timestamp = in.f0;
            if (in.f1 > sensorData.getTs())
                stop_timestamp = in.f1;
            else
                stop_timestamp = sensorData.getTs();
            return new Tuple4<>(start_timestamp, stop_timestamp, sensorData,in.f3+1);
        }
        else
            return new Tuple4<>(sensorData.getTs(),sensorData.getTs(), sensorData,1L);

    }


}

package operator.fold;

import model.SensorData;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Created by marco on 04/07/17.
 */
public class AverageFF implements FoldFunction<SensorData, Tuple5<Long, Long,SensorData,Double,Long>> {

    private boolean compute;

    public AverageFF(boolean compute){
        this.compute = compute;
    }

    @Override
    public Tuple5<Long, Long,SensorData,Double,Long> fold(Tuple5<Long, Long,SensorData,Double,Long> in, SensorData sensorData) throws Exception {
        if(in.f2 != null) {
            sensorData.setV(in.f2.getV() + (sensorData.getV() - in.f2.getV()) / (in.f4 + 1));
            Double currentTotalDistance = Double.valueOf(0);
            if (compute)
                currentTotalDistance = in.f3 + sensorData.computeDistance(sensorData.getX()-in.f2.getX(),sensorData.getY()-in.f2.getY());
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
            return new Tuple5<>(start_timestamp, stop_timestamp, sensorData,currentTotalDistance,in.f4+1);
        }
        else
            return new Tuple5<>(sensorData.getTs(),sensorData.getTs(), sensorData,new Double(0),1L);

    }


}

package operator.fold;

import configuration.AppConfiguration;
import model.SensorData;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.List;


public class HeatMapFF implements FoldFunction<SensorData, Tuple4<Long, String,List<Long>,Long>> {



    @Override
    public Tuple4<Long, String,List<Long>,Long> fold(Tuple4<Long, String,List<Long>,Long> in, SensorData sensorData) throws Exception {
        if(in.f2 != null) {
            int x_ceil = computeX(sensorData.getX());
            int y_ceil = computeY(sensorData.getY());
            if (x_ceil > -1 && y_ceil > -1)
                in.f2.set( (y_ceil*AppConfiguration.HORIZONTAL_CEILS) + x_ceil,
                        in.f2.get((y_ceil*AppConfiguration.HORIZONTAL_CEILS) + x_ceil) + 1);
            long start_timestamp = 0;
            if (in.f0 >= sensorData.getTs())
                start_timestamp = sensorData.getTs();
            else
                start_timestamp = in.f0;
            return new Tuple4<>(start_timestamp, in.f1,in.f2,in.f3+1);
        }
        else{
            List<Long> heatMap = initMap();
            int x_ceil = computeX(sensorData.getX());
            int y_ceil = computeY(sensorData.getY());
            if (x_ceil > -1 && y_ceil > -1)
                heatMap.set( (y_ceil*AppConfiguration.HORIZONTAL_CEILS) + x_ceil,
                        heatMap.get((y_ceil*AppConfiguration.HORIZONTAL_CEILS) + x_ceil) + 1);
             return new Tuple4<>(sensorData.getTs(),String.valueOf(sensorData.getSid()), heatMap,1L);
        }

    }

    private List<Long> initMap() {
        List<Long> heatMap = new ArrayList<>();
        for (int i = 0 ; i < AppConfiguration.HORIZONTAL_CEILS*AppConfiguration.VERTICAL_CEILS ; i++)
            heatMap.add(0L);
        return heatMap;
    }

    private Integer computeX(long x){
        int i = 1;
        while ( i <= AppConfiguration.HORIZONTAL_CEILS){
            if ( x < AppConfiguration.X_MIN_FIELD + i*AppConfiguration.X_STEP)
                return i-1;//i-1
            i++;
        }
        return -1;
    }

    private Integer computeY(long y){
        int i = 1;
        while ( i <= AppConfiguration.VERTICAL_CEILS){
            if ( y >  AppConfiguration.Y_MAX_FIELD - i*AppConfiguration.Y_STEP)
                return i-1;//i-1
            i++;
        }
        return -1;
    }
}

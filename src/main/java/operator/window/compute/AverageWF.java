package operator.window.compute;

import model.SensorData;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import utils.DatasetMap;

/**
 * Created by marco on 10/07/17.
 */
public class AverageWF implements WindowFunction<SensorData,Tuple5<Long,Long,String,Double,Double>,String,Window> {



    @Override
    public void apply(String key, Window window, Iterable<SensorData> iterable, Collector<Tuple5<Long, Long,String, Double, Double>> collector) throws Exception {
        Tuple5<Long,Long,String,Double,Double> returnTuple = new Tuple5<>();
        if (DatasetMap.getDatasetMap() == null)
            DatasetMap.initMap();
        returnTuple.f2 = DatasetMap.getDatasetMap().get(Long.parseLong(key));
        returnTuple.f0 = 0L;
        returnTuple.f1 = 0L;
        returnTuple.f4 = 0d;
        long i = 0;
        for (SensorData sensorData : iterable){
            if (returnTuple.f0 == 0)
                returnTuple.f0 = sensorData.getTs();
            if (returnTuple.f0 > sensorData.getTs())
                returnTuple.f0 = sensorData.getTs();
            if (returnTuple.f1 == 0)
                returnTuple.f1 = sensorData.getTs();
            if (returnTuple.f1 < sensorData.getTs())
                returnTuple.f1 = sensorData.getTs();
            returnTuple.f4+=sensorData.getV();
            i++;
        }
        returnTuple.f4 = returnTuple.f4/i;
        returnTuple.f3 = returnTuple.f4*(returnTuple.f1-returnTuple.f0)/1000;
        collector.collect(returnTuple);


    }
}
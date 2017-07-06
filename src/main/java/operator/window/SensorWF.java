package operator.window;


import model.SensorData;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by marco on 04/07/17.
 */
public class SensorWF implements WindowFunction<Tuple5<Long,Long,SensorData,Double,Long>,Tuple5<Long,Long,String,Double,Double>,String,Window> {



    @Override
    public void apply(String key, Window window, Iterable<Tuple5<Long,Long,SensorData,Double,Long>> iterable, Collector<Tuple5<Long, Long,String, Double, Double>> collector) throws Exception {
        Tuple5<Long,Long,SensorData,Double,Long> lastestTuple = iterable.iterator().next();
        /**
         * Compute distance as speed*WindowTime - Must be changed to total distance
         */
        collector.collect(new Tuple5<>(lastestTuple.f0,lastestTuple.f1,
                lastestTuple.f2.getKey(),lastestTuple.f2.getV()*((lastestTuple.f1-lastestTuple.f0)/1000), lastestTuple.f2.getV()));
    }
}

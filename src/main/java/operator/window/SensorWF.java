package operator.window;


import model.SensorData;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by marco on 04/07/17.
 */
public class SensorWF implements WindowFunction<Tuple5<SensorData,Long,Double,Long,Long>,Tuple5<Long,Long,String,Double,Double>,String,Window> {



    @Override
    public void apply(String key, Window window, Iterable<Tuple5<SensorData, Long, Double,Long, Long>> iterable, Collector<Tuple5<Long, Long,String, Double, Double>> collector) throws Exception {
        Tuple5<SensorData, Long,Double,Long,Long> lastestTuple = iterable.iterator().next();
        collector.collect(new Tuple5<>(lastestTuple.f3,lastestTuple.f4,
                lastestTuple.f0.getKey(),lastestTuple.f2, lastestTuple.f0.getV()));
    }
}

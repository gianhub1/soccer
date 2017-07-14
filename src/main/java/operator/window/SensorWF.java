package operator.window;


import model.SensorData;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;


public class SensorWF implements WindowFunction<Tuple4<Long,Long,SensorData,Long>,Tuple5<Long,Long,String,Double,Double>,String,Window> {



    @Override
    public void apply(String key, Window window, Iterable<Tuple4<Long,Long,SensorData,Long>> iterable, Collector<Tuple5<Long, Long,String, Double, Double>> collector) throws Exception {
        Tuple4<Long,Long,SensorData,Long> lastestTuple = iterable.iterator().next();
        collector.collect(new Tuple5<>(lastestTuple.f0,lastestTuple.f1,
                lastestTuple.f2.getKey(),lastestTuple.f2.getV()*((lastestTuple.f1-lastestTuple.f0)/1000), lastestTuple.f2.getV()));
    }
}

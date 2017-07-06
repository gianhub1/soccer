package operator.window;


import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by marco on 04/07/17.
 */
public class PlayerWF implements WindowFunction<Tuple5<Long,Long,String,Double,Double>,Tuple5<Long,Long,String,Double,Double>,String,Window> {



    @Override
    public void apply(String key, Window window, Iterable<Tuple5<Long, Long, String,Double, Double>> iterable, Collector<Tuple5<Long, Long,String, Double, Double>> collector) throws Exception {
        collector.collect(iterable.iterator().next());
    }
}

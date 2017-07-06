package operator.window;


import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by marco on 04/07/17.
 */
public class PlayerWF implements WindowFunction<Tuple6<Long,Long,String,Double,Double,Long>,Tuple5<Long,Long,String,Double,Double>,String,Window> {



    @Override
    public void apply(String key, Window window, Iterable<Tuple6<Long, Long, String,Double, Double,Long>> iterable, Collector<Tuple5<Long, Long,String, Double, Double>> collector) throws Exception {
        Tuple6<Long, Long,String, Double, Double,Long> latest = iterable.iterator().next();
        collector.collect(new Tuple5<>(latest.f0,latest.f1,
                latest.f2,latest.f3,latest.f4));
    }
}

package operator.window.compute;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by marco on 10/07/17.
 */
public class TupleAverageWF implements WindowFunction<Tuple5<Long,Long,String,Double,Double>,Tuple5<Long,Long,String,Double,Double>,String,Window> {

    private boolean average = false;

    public TupleAverageWF(boolean average){
        this.average = average;
    }

    @Override
    public void apply(String key, Window window, Iterable<Tuple5<Long,Long,String,Double,Double>> iterable, Collector<Tuple5<Long, Long,String, Double, Double>> collector) throws Exception {
        Tuple5<Long,Long,String,Double,Double> returnTuple = new Tuple5<>();
        returnTuple.f2 = null;
        returnTuple.f0 = 0L;
        returnTuple.f1 = 0L;
        returnTuple.f4 = 0d;
        returnTuple.f3 = 0d;
        long i = 0;
        for (Tuple5<Long,Long,String,Double,Double> tuple : iterable){
            if (returnTuple.f2 == null)
                returnTuple.f2 = tuple.f2;
            if (returnTuple.f0 == 0)
                returnTuple.f0 = tuple.f0;
            if (returnTuple.f0 > tuple.f0)
                returnTuple.f0 = tuple.f0;
            if (returnTuple.f1 == 0)
                returnTuple.f1 = tuple.f1;
            if (returnTuple.f1 < tuple.f1)
                returnTuple.f1 = tuple.f1;
            returnTuple.f4+=tuple.f4;
            returnTuple.f3+=tuple.f3;
            i++;
        }
        returnTuple.f4 = returnTuple.f4/i;
        if (average)
            returnTuple.f3 = returnTuple.f3/i;
        collector.collect(returnTuple);


    }
}

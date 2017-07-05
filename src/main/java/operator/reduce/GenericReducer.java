package operator.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Created by marco on 05/07/17.
 */
public class GenericReducer implements ReduceFunction<Tuple5<Long,Long,String,Double,Double>> {

    boolean player = false;

    public GenericReducer(boolean player){
        this.player = player;
    }

    @Override
    public Tuple5<Long, Long, String, Double, Double> reduce(Tuple5<Long, Long, String, Double, Double> first, Tuple5<Long, Long, String, Double, Double> second) throws Exception {
        long start_timestamp = 0;
        long end_timestamp = 0;
        if (first.f0 >= second.f0)
            start_timestamp = first.f0;
        else
            start_timestamp = second.f0;
        if (first.f1 >= second.f1)
            end_timestamp = first.f1;
        else
            end_timestamp = second.f1;
        if (this.player){
            return new Tuple5<>(start_timestamp,end_timestamp,first.f2,(first.f3+second.f3),(first.f4+second.f4)/2);
        }
        else
            return new Tuple5<>(start_timestamp,end_timestamp,first.f2,(first.f3+second.f3)/2,(first.f4+second.f4)/2);

    }



}

package operator.fold;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Created by marco on 06/07/17.
 */
public class AggregateFF implements FoldFunction<Tuple5<Long, Long,String,Double,Double>, Tuple6<Long, Long,String,Double,Double,Long>> {

    @Override
    public Tuple6<Long, Long,String,Double,Double,Long> fold(Tuple6<Long, Long,String,Double,Double,Long> in, Tuple5<Long, Long,String,Double,Double> stream) throws Exception {
        if(!in.f2.equals("")) {
            long start_timestamp = 0;
            long stop_timestamp = 0;
            if (in.f0 >= stream.f0)
                start_timestamp = stream.f0;
            else
                start_timestamp = in.f0;
            if (in.f1 > stream.f1)
                stop_timestamp = in.f1;
            else
                stop_timestamp = stream.f1;
            return new Tuple6<>(start_timestamp, stop_timestamp, stream.f2, in.f3 + stream.f3, (in.f4 + (stream.f4 - in.f4) / (in.f5 + 1)), in.f5 + 1);
        }
        else
            return new Tuple6<>(stream.f0,stream.f1, stream.f2,stream.f3,stream.f4,1L);
    }


}
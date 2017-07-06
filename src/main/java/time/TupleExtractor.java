package time;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Created by marco on 06/07/17.
 */
public class TupleExtractor extends AscendingTimestampExtractor<Tuple5<Long,Long,String,Double,Double>> {

    @Override
    public long extractAscendingTimestamp(Tuple5<Long, Long, String, Double, Double> tuple) {
        return (tuple.f1 + tuple.f0)/2;
    }

}

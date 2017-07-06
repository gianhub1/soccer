package time;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Created by marco on 06/07/17.
 */
public class TupleExtractor extends AscendingTimestampExtractor<Tuple5<Long,Long,String,Double,Double>> {


/*
    private static Time delay = Time.minutes(1);
    public TupleExtractor() {
        super(delay);
    }
*/

    @Override
    public long extractAscendingTimestamp(Tuple5<Long, Long, String, Double, Double> tuple) {
        return (tuple.f0+tuple.f1)/2;
    }

/*    @Override
    public long extractTimestamp(Tuple5<Long, Long, String, Double, Double> tuple) {
        return (tuple.f0+tuple.f1)/2;
    }*/
}

package time;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;


public class TupleExtractor extends AscendingTimestampExtractor<Tuple5<Long,Long,String,Double,Double>> {

    @Override
    public long extractAscendingTimestamp(Tuple5<Long, Long, String, Double, Double> tuple) {
        return (tuple.f0+tuple.f1)/2;
    }

}

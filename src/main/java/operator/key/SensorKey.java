package operator.key;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Created by marco on 05/07/17.
 */
public class SensorKey implements KeySelector<Tuple5<Long,Long,String,Double,Double>, String> {

    @Override
    public String getKey(Tuple5<Long,Long,String,Double,Double> tuple) throws Exception {
        return tuple.f2;
    }
}

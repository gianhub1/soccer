package operator.key;

import model.HeatMap;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

/**
 * Created by marco on 07/07/17.
 */
public class HeatMapKey implements KeySelector<Tuple3<Long,String,List<HeatMap>>, String> {

    @Override
    public String getKey(Tuple3<Long,String,List<HeatMap>> tuple) throws Exception {
        return tuple.f1;
    }
}


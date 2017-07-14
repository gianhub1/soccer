package operator.fold;

import configuration.AppConfiguration;
import model.HeatMap;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.List;


public class HeatMapAggregateFF implements FoldFunction<Tuple3<Long,String,List<HeatMap>>, Tuple4<Long, String,List<HeatMap>,List<Long>>> {

    @Override
    public Tuple4<Long, String,List<HeatMap>,List<Long>> fold(Tuple4<Long, String,List<HeatMap>,List<Long>> in, Tuple3<Long,String,List<HeatMap>> stream) throws Exception {
        if(in.f2 != null) {
            long start_timestamp = 0;
            if (in.f0 >= stream.f0)
                start_timestamp = stream.f0;
            else
                start_timestamp = in.f0;
            for (int i = 0 ; i < in.f2.size() ; i++){
                if (i < stream.f2.size()){
                    in.f2.get(i).setPercent_time(in.f2.get(i).getPercent_time() + stream.f2.get(i).getPercent_time());
                    in.f3.set(i,in.f3.get(i)+1);
                }
            }
            return new Tuple4<>(start_timestamp, in.f1, in.f2,in.f3);
        }
        else{
            List<Long> counters = initMap(stream.f2);
            return new Tuple4<>(stream.f0,stream.f1,stream.f2,counters);
        }
    }


    private List<Long> initMap(List<HeatMap> heatMap) {
        List<Long> counter = new ArrayList<>();
        for (int i = 0; i < AppConfiguration.HORIZONTAL_CEILS*AppConfiguration.VERTICAL_CEILS ; i++){
            if (i < heatMap.size() && heatMap.get(i).getPercent_time() > 0)
                counter.add(1L);
            else
                counter.add(0L);
        }
        return counter;
    }


}

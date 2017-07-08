package operator.window;

import model.HeatMap;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Created by marco on 07/07/17.
 */
public class HeatMapAggregateWF implements WindowFunction<Tuple4<Long,String,List<HeatMap>,List<Long>>,Tuple3<Long,String,List<HeatMap>>,String,Window> {


    private boolean log = false;

    public HeatMapAggregateWF(boolean log){
        this.log = log;
    }
    @Override
    public void apply(String key, Window window, Iterable<Tuple4<Long, String, List<HeatMap>, List<Long>>> iterable, Collector<Tuple3<Long, String, List<HeatMap>>> collector) throws Exception {
        Tuple4<Long, String, List<HeatMap>, List<Long>> latest = iterable.iterator().next();
        Tuple3<Long, String, List<HeatMap>> returnValue = new Tuple3<>();
        returnValue.f0 = latest.f0;
        returnValue.f1 = latest.f1;
        for (int i = 0; i < latest.f2.size(); i++){
            if (i < latest.f3.size() && latest.f3.get(i)>0)
                latest.f2.get(i).setPercent_time(latest.f2.get(i).getPercent_time()/latest.f3.get(i));
        }
        returnValue.f2 = latest.f2;
        collector.collect(returnValue);
    }
}
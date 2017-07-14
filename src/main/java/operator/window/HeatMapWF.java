package operator.window;

import model.HeatMap;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import utils.DatasetMap;

import java.util.ArrayList;
import java.util.List;


public class HeatMapWF implements WindowFunction<Tuple4<Long,String,List<Long>,Long>,Tuple3<Long,String,List<HeatMap>>,String,Window> {



    @Override
    public void apply(String key, Window window, Iterable<Tuple4<Long,String,List<Long>,Long>> iterable, Collector<Tuple3<Long,String,List<HeatMap>>> collector) throws Exception {
        Tuple4<Long,String,List<Long>,Long> latest = iterable.iterator().next();
        Tuple3<Long,String,List<HeatMap>> returnValue = new Tuple3<>();
        returnValue.f0 = latest.f0;
        if (DatasetMap.getDatasetMap() == null)
            DatasetMap.initMap();
        returnValue.f1 = DatasetMap.getDatasetMap().get(Long.parseLong(key));
        List<HeatMap> heatMap = new ArrayList<>();
        for (int i = 0 ; i < latest.f2.size() ; i++ ){
            if (latest.f3 > 0 )
                heatMap.add(new HeatMap(i, ((double)latest.f2.get(i))/latest.f3*100));
        }
        returnValue.f2 = heatMap;
        collector.collect(returnValue);
    }
}
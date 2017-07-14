package operator.flatmap;

import model.SensorData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import utils.DatasetMap;


public class StringMapperFD implements FlatMapFunction<String, SensorData> {
    @Override
    public void flatMap(String o, Collector collector) throws Exception {
        if (DatasetMap.getDatasetMap() == null)
            DatasetMap.initMap();
        String[] params = o.split(",");
        long x = Long.parseLong(params[2]);
        long y = Long.parseLong(params[3]);
        if (x < 0 )
            x += 50;
        if (x > 52477)
            x -= 11;
        if (y > 33941)
            y -= 24;
        if (y < -33939)
            y += 21;
        SensorData sensorData = new SensorData(DatasetMap.getDatasetMap().get(Long.parseLong(params[0]))
                ,Long.parseLong(params[1]), x,y,
                Double.parseDouble(params[5])/1000000,Long.parseLong(params[0]));
        collector.collect(sensorData);

    }
}
package operator.flatmap;

import model.SensorData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;


public class StringMapper implements FlatMapFunction<String, SensorData>{
    @Override
    public void flatMap(String o, Collector collector) throws Exception {
        String[] params = o.split(",");
        SensorData sensorData = new SensorData(params[1],Long.parseLong(params[2]),
                Long.parseLong(params[3]),Long.parseLong(params[4]),Double.parseDouble(params[5]),Long.parseLong(params[0]));
        collector.collect(sensorData);

    }
}

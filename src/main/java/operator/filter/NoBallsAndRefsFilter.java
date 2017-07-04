package operator.filter;

import configuration.AppConfiguration;
import model.SensorData;
import org.apache.flink.api.common.functions.FilterFunction;
import utils.DatasetMap;

/**
 * Created by marco on 24/06/17.
 */
public class NoBallsAndRefsFilter implements FilterFunction<SensorData> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(SensorData sensorData) throws Exception {
        return (sensorData!=null && !isPlayer(sensorData.getSid()) && !prePostMatchEvent(sensorData.getTs()));
    }

    public boolean isPlayer(long sid){
        return ((DatasetMap.getDatasetMap().get(sid)==null || DatasetMap.getDatasetMap().get(sid).equals("Ball")
        || DatasetMap.getDatasetMap().get(sid).equals("Hand") || DatasetMap.getDatasetMap().get(sid).equals("Referee")));
    }

    public boolean prePostMatchEvent(long timestamp){
        return (timestamp < AppConfiguration.TS_MATCH_START || timestamp > AppConfiguration.TS_MATCH_STOP);
    }

}
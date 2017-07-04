package schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.SensorData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;

/**
 * Created by marco on 24/06/17.
 */
public class SensorDataSchema implements DeserializationSchema<SensorData>, SerializationSchema<SensorData> {

    public ObjectMapper mapper;

    @Override
    public byte[] serialize(SensorData element) {

        this.mapper = new ObjectMapper();

        String jsonInString = new String("");
        try {

            jsonInString = mapper.writeValueAsString(element);
            return jsonInString.getBytes();

        } catch (JsonProcessingException e) {

            return null;

        }

    }

    @Override
    public SensorData deserialize(byte[] message) {

        String jsonInString = new String(message);
        this.mapper = new ObjectMapper();

        try {
            SensorData sensorData = this.mapper.readValue(jsonInString, SensorData.class);
            return sensorData;

        }  catch (IOException e) {
            e.printStackTrace();
            return null;

        }

    }

    @Override
    public boolean isEndOfStream(SensorData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<SensorData> getProducedType() {
        return TypeExtractor.getForClass(SensorData.class);
    }
}
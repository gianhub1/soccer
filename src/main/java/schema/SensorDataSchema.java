package schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.SensorData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;


public class SensorDataSchema implements DeserializationSchema<String>, SerializationSchema<SensorData> {

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
    public String deserialize(byte[] message) {

        return new String(message);

    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeExtractor.getForClass(String.class);
    }
}

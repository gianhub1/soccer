package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by marco on 24/06/17.
 */
public class JSONFormatter {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static String serialize(Object o) {

        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
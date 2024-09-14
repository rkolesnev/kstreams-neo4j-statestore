package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer <T> implements Deserializer<T> {
  private final Class<T> clazz;
  private final ObjectMapper objectMapper;

  public JsonDeserializer(Class<T> clazz) {
    this.objectMapper = new ObjectMapper();
    this.clazz=clazz;
  }

  public T deserialize(byte[] data) {
    if (data == null || data.length == 0) {
      return null;
    }
    try {
      return objectMapper.readValue(data, clazz);
    } catch (IOException e) {
      throw new RuntimeException("Error deserializing data from JSON", e);
    }
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    return deserialize(data);
  }
}

package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

  private final ObjectMapper objectMapper;

  public JsonSerializer() {
    this.objectMapper = new ObjectMapper();
  }

  public byte[] serialize(T data) {
    if (data == null) {
      return null;
    }
    try {

      return objectMapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing data into JSON", e);
    }
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return serialize(data);
  }
}

package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerDe<T> implements Serde<T> {

  private final Class<T> clazz;
  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public JsonSerDe(Class<T> clazz) {
    this.clazz = clazz;
    this.deserializer = new JsonDeserializer<>(clazz);
    this.serializer = new JsonSerializer<>();
  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }
}

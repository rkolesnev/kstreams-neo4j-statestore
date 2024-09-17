package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class JsonSerDe<T> implements Serde<T>,Serializer<T>,Deserializer<T> {

  private final Class<T> clazz;
  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public JsonSerDe(Class<T> clazz) {
    this.clazz = clazz;
    this.deserializer = new JsonDeserializer<>(clazz);
    this.serializer = new JsonSerializer<>();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serde.super.configure(configs, isKey);
  }

  @Override
  public void close() {
    Serde.super.close();
  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    return deserializer.deserialize(s,bytes);
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {
    return Deserializer.super.deserialize(topic, headers, data);
  }

  @Override
  public T deserialize(String topic, Headers headers, ByteBuffer data) {
    return Deserializer.super.deserialize(topic, headers, data);
  }

  @Override
  public byte[] serialize(String s, T t) {
    return serializer.serialize(s,t);
  }
}

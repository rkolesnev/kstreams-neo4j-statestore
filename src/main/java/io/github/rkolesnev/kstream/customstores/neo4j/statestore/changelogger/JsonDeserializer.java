package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.jsontype.DefaultBaseTypeLimitingValidator;
import java.io.IOException;

public class JsonDeserializer {

  private final ObjectMapper objectMapper;

  public JsonDeserializer() {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.activateDefaultTypingAsProperty(new DefaultBaseTypeLimitingValidator(),
        DefaultTyping.NON_FINAL, "class");
  }

  public ChangeLogEntry deserialize(byte[] data) {
    if (data == null || data.length == 0) {
      return null;
    }
    try {
      return objectMapper.readValue(data, ChangeLogEntry.class);
    } catch (IOException e) {
      throw new RuntimeException("Error deserializing data from JSON", e);
    }
  }

}

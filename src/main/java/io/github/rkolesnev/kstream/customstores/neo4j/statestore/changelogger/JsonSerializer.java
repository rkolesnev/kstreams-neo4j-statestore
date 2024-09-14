package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.jsontype.DefaultBaseTypeLimitingValidator;

public class JsonSerializer {

  private final ObjectMapper objectMapper;

  public JsonSerializer() {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.activateDefaultTypingAsProperty(new DefaultBaseTypeLimitingValidator(),
        DefaultTyping.NON_FINAL, "class");
  }

  public byte[] serialize(ChangeLogEntry data) {
    if (data == null) {
      return null;
    }
    try {

      return objectMapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing data into JSON", e);
    }
  }
}

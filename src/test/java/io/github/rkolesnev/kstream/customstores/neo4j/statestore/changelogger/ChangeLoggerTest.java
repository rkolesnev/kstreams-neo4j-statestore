package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.DefaultBaseTypeLimitingValidator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class ChangeLoggerTest {

  ObjectMapper objectMapper = new ObjectMapper();

  @Test
  @SneakyThrows
  void testSerDes() {
    objectMapper.activateDefaultTypingAsProperty(new DefaultBaseTypeLimitingValidator(),ObjectMapper.DefaultTyping.NON_FINAL, "class");
    String addNode = objectMapper.writeValueAsString(new AddNodeOp("node1"));
    log.info("json: {}", addNode);

    ChangeLogEntry changeLogEntry = objectMapper.readValue(addNode, ChangeLogEntry.class);
    if(changeLogEntry instanceof AddNodeOp) {
      AddNodeOp op = (AddNodeOp) changeLogEntry;
      log.info("op nodeKey: {}", op.getKey());
    }
    log.info("json: {}", addNode);
    log.info("ChangeLogEntry: {}", changeLogEntry);
    String addNode2 = objectMapper.writeValueAsString(changeLogEntry);
    log.info("json: {}", addNode2);
  }
}
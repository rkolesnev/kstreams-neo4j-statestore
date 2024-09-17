package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.NoopChangeLogger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.RelationshipType;

@Slf4j
class Neo4jStoreTest {
  Path tempPath;
  Neo4jStore store;

  @SneakyThrows
  @BeforeEach
  void setup() {
    tempPath = Files.createTempDirectory("neo4jTests");
    store = new Neo4jStore(new NoopChangeLogger(), tempPath);
  }

  @SneakyThrows
  @AfterEach
  void tearDown(){
    store.close();
    FileUtils.deleteDirectory(tempPath.toFile());
  }

  @Test
  void test2() {
    RelationshipType relationshipType = RelationshipType.withName("rel");
    NodePojo first = store.createNode("first", Map.of("prop1", "blah"));
    NodePojo second = store.createNode("second", Map.of());
    RelationshipPojo rel = store.relateNodes("first", "second", relationshipType,
        Map.of("relProp", "blah"));
    RelationshipPojo again = store.relateNodes("first", "second", relationshipType,
        Map.of("relProp", "blah2"));
    store.relateNodes("first", "second", relationshipType, Map.of("relProp", "blah2"));
    store.relateNodes("first", "second", relationshipType, Map.of("relProp", "blah2"));
    store.relateNodes("first", "second", relationshipType, Map.of("relProp", "blah2"));
    log.info("Related nodes: {}",
        store.findRelatedNodes("first", TraversalParameters.builder().build()));
  }

  @Test
  void testWalkingTheGraphAnyDirection() {
    List<String> nodeKeys = List.of("Node0", "Node1", "Node2", "Node3");
    List<String> extraNodesAtLvl2 = List.of("Node2-1", "Node2-2");
    for (String key : nodeKeys) {
      store.createNode(key, Map.of("NewProp", "Key-" + key));
    }
    for (String key : extraNodesAtLvl2) {
      store.createNode(key, Map.of("NewProp", "Key-" + key));
    }
    RelationshipType relationshipTypeOut = RelationshipType.withName("Outbound");
    store.relateNodes(nodeKeys.get(0), nodeKeys.get(1), relationshipTypeOut, Map.of());

    //reverse relationship
    RelationshipType relationshipTypeIn = RelationshipType.withName("Inbound");
    store.relateNodes(nodeKeys.get(2), nodeKeys.get(1), relationshipTypeIn, Map.of());
    //back to outbound
    store.relateNodes(nodeKeys.get(2), nodeKeys.get(3), relationshipTypeOut, Map.of());

    //extra nodes all related from node1
    for (String key : extraNodesAtLvl2) {
      store.relateNodes(nodeKeys.get(1), key, relationshipTypeOut, Map.of());
    }

    TraversalResult traversalResult = store.findRelatedNodes(nodeKeys.get(0),
        TraversalParameters.builder().build());
    log.info("TraversalResult: {}", traversalResult);
    Map<Integer, Set<String>> expectedResult = Map.of(1, Set.of("Node1"), 2,
        Set.of("Node2", "Node2-1", "Node2-2"), 3, Set.of("Node3"));
    Map<Integer, Set<String>> actualResult = traversalResult.getFoundNodesWithLevel().entrySet().stream()
        .map(e -> new SimpleEntry<>(e.getKey(), e.getValue().stream().map(
            NodePojo::key).collect(
            Collectors.toSet()))).collect(
            Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    assertThat(actualResult).containsExactlyInAnyOrderEntriesOf(expectedResult);
  }

  @Test
  void relationshipConstraintTest() {

    NodePojo first = store.createNode("first", Map.of("prop1", "blah"));
    NodePojo second = store.createNode("second", Map.of());
    NodePojo third = store.createNode("third", Map.of());
    store.relateNodes("first", "second", RelationshipType.withName("rel"), Map.of("rkey", "r1"));
    assertThatThrownBy(() -> store.relateNodes("second", "third", RelationshipType.withName("rel"),
        Map.of("rkey", "r1"))).isInstanceOf(
        ConstraintViolationException.class);
  }
}
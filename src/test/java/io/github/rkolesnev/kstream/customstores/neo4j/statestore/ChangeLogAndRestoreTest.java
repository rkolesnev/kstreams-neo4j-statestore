package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rkolesnev.kstream.customstores.neo4j.statestore.RelationshipPojo.Direction;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.ChangeLogger;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.NoopChangeLogger;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorMetadata;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.RelationshipType;

public class ChangeLogAndRestoreTest {

  Path tempPath;

  Neo4jStore store;
  Neo4jStore store2;

  TestContext context = new TestContext();
  ChangeLogger changeLogger = new ChangeLogger(context, "blah", () -> Position.emptyPosition());
  Restorer restorer;

  @SneakyThrows
  @BeforeEach
  void setup() {
    tempPath = Files.createTempDirectory("neo4jTests");
    store = new Neo4jStore(new NoopChangeLogger(), tempPath);
    store2 = new Neo4jStore(changeLogger, tempPath);
    restorer= new Restorer(store2);
  }

  @SneakyThrows
  @AfterEach
  void tearDown(){
    store.close();
    store2.close();
    FileUtils.deleteDirectory(tempPath.toFile());
  }

  @Test
  void testChangeloggingCreateNode() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    assertThat(context.replayList).hasSize(2);
    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    NodePojo nodePojo = store2.getNodeByKey("Node1");
    assertThat(nodePojo.key()).isEqualTo("Node1");
    assertThat(nodePojo.properties()).containsExactly(new SimpleEntry<>("prop1", "val1"));
    assertThat(context.replayList).hasSize(2);
  }

  @Test
  void testChangeloggingDeleteNode() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    assertThat(context.replayList).hasSize(2);

    store.deleteNode("Node1", false);
    assertThat(context.replayList).hasSize(6);
    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    NodePojo nodePojo = store2.getNodeByKey("Node1");
    assertThat(nodePojo).isNull();
    assertThat(context.replayList).hasSize(6);
  }

  @Test
  void testChangeloggingAddNodeProp() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    assertThat(context.replayList).hasSize(2);

    store.setNodeProperty("Node1", "prop2", "val2");
    assertThat(context.replayList).hasSize(3);
    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    NodePojo nodePojo = store2.getNodeByKey("Node1");
    assertThat(nodePojo.properties()).containsOnly(new SimpleEntry<>("prop1", "val1"),
        new SimpleEntry<>("prop2", "val2"));
    assertThat(context.replayList).hasSize(3);
  }

  @Test
  void testChangeloggingDeleteNodeProp() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    assertThat(context.replayList).hasSize(2);

    store.setNodeProperty("Node1", "prop2", "val2");
    assertThat(context.replayList).hasSize(3);
    store.removePropertyFromNode("Node1", "prop1");
    assertThat(context.replayList).hasSize(5);
    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    NodePojo nodePojo = store2.getNodeByKey("Node1");
    assertThat(nodePojo.properties()).containsOnly(
        new SimpleEntry<>("prop2", "val2"));
    assertThat(context.replayList).hasSize(5);
  }

  @Test
  void testChangeloggingAddRelationship() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    store.createNode("Node2", Map.of());
    assertThat(context.replayList).hasSize(3);
    String relKey = store.relateNodes("Node1", "Node2", RelationshipType.withName("rel"), Map.of())
        .getRelationshipKey();
    assertThat(context.replayList).hasSize(4);
    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    List<RelationshipPojo> node1Relationships = store2.getNodeRelationships("Node1");
    List<RelationshipPojo> node2Relationships = store2.getNodeRelationships("Node2");
    assertThat(node1Relationships).hasSize(1);
    assertThat(node2Relationships).hasSize(1);
    assertThat(node1Relationships.get(0)).isEqualTo(
        new RelationshipPojo(relKey, "rel", "Node1", "Node2", Map.of(),
            Direction.OUT));
    assertThat(node2Relationships.get(0)).isEqualTo(
        new RelationshipPojo(relKey, "rel", "Node1", "Node2", Map.of(),
            Direction.IN));
    assertThat(context.replayList).hasSize(4);
  }


  @Test
  void testChangeloggingAddRelationshipWithProperties() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    store.createNode("Node2", Map.of());
    assertThat(context.replayList).hasSize(3);
    String relKey = store.relateNodes("Node1", "Node2", RelationshipType.withName("rel"),
        Map.of("prop1", "val1", "rprop2", "val2")).getRelationshipKey();
    assertThat(context.replayList).hasSize(6);
    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    List<RelationshipPojo> node1Relationships = store2.getNodeRelationships("Node1");
    List<RelationshipPojo> node2Relationships = store2.getNodeRelationships("Node2");
    assertThat(node1Relationships).hasSize(1);
    assertThat(node2Relationships).hasSize(1);
    assertThat(node1Relationships.get(0)).isEqualTo(
        new RelationshipPojo(relKey, "rel", "Node1", "Node2",
            Map.of("prop1", "val1", "rprop2", "val2"),
            Direction.OUT));
    assertThat(node2Relationships.get(0)).isEqualTo(
        new RelationshipPojo(relKey, "rel", "Node1", "Node2",
            Map.of("prop1", "val1", "rprop2", "val2"),
            Direction.IN));
    assertThat(context.replayList).hasSize(6);
  }


  @Test
  void testChangeloggingDeleteRelationshipWithProperties() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    store.createNode("Node2", Map.of());
    assertThat(context.replayList).hasSize(3);
    String relKey = store.relateNodes("Node1", "Node2", RelationshipType.withName("rel"),
        Map.of("prop1", "val1", "rprop2", "val2")).getRelationshipKey();
    assertThat(context.replayList).hasSize(6);
    store.deleteRelationship(relKey, RelationshipType.withName("rel"));
    assertThat(context.replayList).hasSize(12);

    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    List<RelationshipPojo> node1Relationships = store2.getNodeRelationships("Node1");
    List<RelationshipPojo> node2Relationships = store2.getNodeRelationships("Node2");
    assertThat(node1Relationships).hasSize(0);
    assertThat(node2Relationships).hasSize(0);
    assertThat(context.replayList).hasSize(12);
  }

  @Test
  void testChangeloggingDeleteNodeWithRelationshipWithProperties() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    store.createNode("Node2", Map.of());
    assertThat(context.replayList).hasSize(3);
    String relKey = store.relateNodes("Node1", "Node2", RelationshipType.withName("rel"),
        Map.of("prop1", "val1", "rprop2", "val2")).getRelationshipKey();
    assertThat(context.replayList).hasSize(6);
    store.deleteNode("Node2", true);
    assertThat(context.replayList).hasSize(14);

    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    List<RelationshipPojo> node1Relationships = store2.getNodeRelationships("Node1");
    assertThat(node1Relationships).hasSize(0);
    assertThat(context.replayList).hasSize(14);
  }

  @Test
  void testChangeloggingAddRelationshipProperties() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    store.createNode("Node2", Map.of());
    assertThat(context.replayList).hasSize(3);
    String relKey = store.relateNodes("Node1", "Node2", RelationshipType.withName("rel"),
        Map.of("prop1", "val1", "rprop2", "val2")).getRelationshipKey();
    assertThat(context.replayList).hasSize(6);
    store.addPropertyToRelationship(RelationshipType.withName("rel"), relKey, "newprop",
        "newvalue");
    assertThat(context.replayList).hasSize(7);

    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    List<RelationshipPojo> node1Relationships = store2.getNodeRelationships("Node1");
    List<RelationshipPojo> node2Relationships = store2.getNodeRelationships("Node2");

    assertThat(node1Relationships).hasSize(1);
    assertThat(node2Relationships).hasSize(1);

    assertThat(node1Relationships.get(0)).isEqualTo(
        new RelationshipPojo(relKey, "rel", "Node1", "Node2",
            Map.of("prop1", "val1", "rprop2", "val2", "newprop", "newvalue"),
            Direction.OUT));
    assertThat(node2Relationships.get(0)).isEqualTo(
        new RelationshipPojo(relKey, "rel", "Node1", "Node2",
            Map.of("prop1", "val1", "rprop2", "val2", "newprop", "newvalue"),
            Direction.IN));
    assertThat(context.replayList).hasSize(7);
  }

  @Test
  void testChangeloggingRemoveRelationshipProperties() {

    store.createNode("Node1", Map.of("prop1", "val1"));
    store.createNode("Node2", Map.of());
    assertThat(context.replayList).hasSize(3);
    String relKey = store.relateNodes("Node1", "Node2", RelationshipType.withName("rel"),
        Map.of("prop1", "val1", "rprop2", "val2")).getRelationshipKey();
    assertThat(context.replayList).hasSize(6);
    store.addPropertyToRelationship(RelationshipType.withName("rel"), relKey, "newprop",
        "newvalue");
    assertThat(context.replayList).hasSize(7);
    store.removePropertyFromRelationship(RelationshipType.withName("rel"), relKey, "prop1");
    assertThat(context.replayList).hasSize(9);
    context.replayList.forEach(
        bytesPair -> restorer.restore(bytesPair.getKey().get(), bytesPair.getValue()));
    List<RelationshipPojo> node1Relationships = store2.getNodeRelationships("Node1");
    List<RelationshipPojo> node2Relationships = store2.getNodeRelationships("Node2");

    assertThat(node1Relationships).hasSize(1);
    assertThat(node2Relationships).hasSize(1);

    assertThat(node1Relationships.get(0)).isEqualTo(
        new RelationshipPojo(relKey, "rel", "Node1", "Node2",
            Map.of("rprop2", "val2", "newprop", "newvalue"),
            Direction.OUT));
    assertThat(node2Relationships.get(0)).isEqualTo(
        new RelationshipPojo(relKey, "rel", "Node1", "Node2",
            Map.of("rprop2", "val2", "newprop", "newvalue"),
            Direction.IN));
    assertThat(context.replayList).hasSize(9);
  }

  class TestContext implements InternalProcessorContext<Object, Object> {

    List<Pair<Bytes, byte[]>> replayList = new ArrayList<>();

    @Override
    public String applicationId() {
      return "";
    }

    @Override
    public TaskId taskId() {
      return null;
    }

    @Override
    public Optional<RecordMetadata> recordMetadata() {
      return Optional.empty();
    }

    @Override
    public Serde<?> keySerde() {
      return null;
    }

    @Override
    public Serde<?> valueSerde() {
      return null;
    }

    @Override
    public File stateDir() {
      return null;
    }

    @Override
    public StreamsMetricsImpl metrics() {
      return null;
    }

    @Override
    public void register(StateStore store, StateRestoreCallback stateRestoreCallback) {

    }

    @Override
    public <S extends StateStore> S getStateStore(String name) {
      return null;
    }

    @Override
    public void register(StateStore store, StateRestoreCallback stateRestoreCallback,
        CommitCallback commitCallback) {

    }

    @Override
    public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
      return null;
    }

    @Override
    public <K, V> void forward(K key, V value) {

    }

    @Override
    public <K, V> void forward(K key, V value, To to) {

    }

    @Override
    public void commit() {

    }

    @Override
    public String topic() {
      return "";
    }

    @Override
    public int partition() {
      return 0;
    }

    @Override
    public long offset() {
      return 0;
    }

    @Override
    public Headers headers() {
      return null;
    }

    @Override
    public long timestamp() {
      return 0;
    }

    @Override
    public Map<String, Object> appConfigs() {
      return Map.of();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(String prefix) {
      return Map.of();
    }

    @Override
    public long currentSystemTimeMs() {
      return 0;
    }

    @Override
    public long currentStreamTimeMs() {
      return 0;
    }


    @Override
    public void setSystemTimeMs(long timeMs) {

    }

    @Override
    public ProcessorRecordContext recordContext() {
      return null;
    }

    @Override
    public void setRecordContext(ProcessorRecordContext recordContext) {

    }

    @Override
    public ProcessorNode<?, ?, ?, ?> currentNode() {
      return null;
    }

    @Override
    public ThreadCache cache() {
      return null;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void uninitialize() {

    }

    @Override
    public TaskType taskType() {
      return null;
    }

    @Override
    public void transitionToActive(StreamTask streamTask, RecordCollector recordCollector,
        ThreadCache newCache) {

    }

    @Override
    public void transitionToStandby(ThreadCache newCache) {

    }

    @Override
    public void registerCacheFlushListener(String namespace, DirtyEntryFlushListener listener) {

    }


    @Override
    public void logChange(String storeName, Bytes key, byte[] value, long timestamp,
        Position position) {
      replayList.add(Pair.of(key, value));
    }

    @Override
    public String changelogFor(String storeName) {
      return "";
    }

    @Override
    public void addProcessorMetadataKeyValue(String key, long value) {

    }

    @Override
    public Long processorMetadataForKey(String key) {
      return 0L;
    }

    @Override
    public void setProcessorMetadata(ProcessorMetadata metadata) {

    }

    @Override
    public ProcessorMetadata getProcessorMetadata() {
      return null;
    }

    @Override
    public void setCurrentNode(ProcessorNode currentNode) {

    }

    @Override
    public void forward(FixedKeyRecord record) {

    }

    @Override
    public void forward(FixedKeyRecord record, String childName) {

    }

    @Override
    public void forward(Record record) {

    }

    @Override
    public void forward(Record record, String childName) {

    }
  }
}

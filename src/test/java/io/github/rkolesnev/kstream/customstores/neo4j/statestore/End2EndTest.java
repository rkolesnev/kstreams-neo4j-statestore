package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters.RelationshipTraversalDirection;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters.TraversalOrder;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters.TraversalParametersBuilder;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.NodeGraphProcessor;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.CreatePersonOp;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.FollowPersonOp;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.NodeInputJsonSerde;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.NodeInputPojo;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.NodeInputPojo.OpType;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.RelationshipPojo.Direction;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.PersonPojo;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.RelationshipType;

@Slf4j
public class End2EndTest {

  private static final String NODES_INPUT_TOPIC = "NODES_IN";
  private static final String MESSAGES_INPUT_TOPIC = "MSG_IN";
  private static final String MESSAGES_OUT_TOPIC = "MSG_OUT";
  private static final String NODES_OP_STATUS_OUT = "NODES_OP_STATUS_OUT";

  private CommonTestUtils commonTestUtils;
  CountDownLatch streamsLatch = new CountDownLatch(1);

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
  }

  @AfterEach
  void teardown() {
    streamsLatch.countDown();
    commonTestUtils.stopKafkaContainer();
  }


  class TestNeo4JStoreSupplier extends Neo4jStoreSupplier {

    Neo4jStore neo4jStore;

    public TestNeo4JStoreSupplier(String name) {
      super(name);
    }

    @Override
    public Neo4jStore get() {
      this.neo4jStore = super.get();
      return this.neo4jStore;
    }
  }

  @Test
  void testEndToEndFlow() {
    NodeInputJsonSerde nodeInputJsonSerde = new NodeInputJsonSerde();
    RelationshipType relationshipType = RelationshipType.withName("Follows");
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    Properties streamsConfig = commonTestUtils.getPropertiesForStreams();
    String kstreamsAppID = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
    String stateStoreName = "Neo4JStoreForDemo";
    int searchDepth = 4;
    TestNeo4JStoreSupplier storeSupplier = new TestNeo4JStoreSupplier(stateStoreName);
    streamsBuilder.addStateStore(new Neo4JStoreBuilder(storeSupplier));
    streamsBuilder.stream(NODES_INPUT_TOPIC,
            Consumed.with(new StringSerde(), nodeInputJsonSerde))
        .process(
            () -> new NodeGraphProcessor(stateStoreName, relationshipType), stateStoreName)
        .to(NODES_OP_STATUS_OUT);
    TopicNameExtractor<String, String> topicNameExtractor = (k, v, recordContext) -> v.split(
        "::")[0];
    streamsBuilder.stream(MESSAGES_INPUT_TOPIC, Consumed.with(new StringSerde(), new StringSerde()))
        .processValues(
            new FixedKeyProcessorSupplier<String, String, Pair<List<String>, String>>() {
              @Override
              public FixedKeyProcessor<String, String, Pair<List<String>, String>> get() {
                return new FixedKeyProcessor<String, String, Pair<List<String>, String>>() {
                  FixedKeyProcessorContext<String, Pair<List<String>, String>> context;
                  Neo4jStore store;

                  public void init(
                      final FixedKeyProcessorContext<String, Pair<List<String>, String>> context) {
                    this.context = context;
                    this.store = context.getStateStore(stateStoreName);
                  }

                  @Override
                  public void process(FixedKeyRecord<String, String> record) {
                    String fromUser = record.key();
                    String message = record.value();
                    TraversalResult traversalResult = store.findRelatedNodes(fromUser,
                        new TraversalParametersBuilder().withTraversalOrder(
                                TraversalOrder.BREADTH_FIRST).withMaxDepth(searchDepth)
                            .addRelationshipToTraverse(relationshipType,
                                RelationshipTraversalDirection.OUT).build());
                    List<String> destinations = traversalResult.getFoundNodesWithLevel().entrySet()
                        .stream()
                        .flatMap(e -> e.getValue().stream().filter(v -> !fromUser.equals(v.key()))
                            .map(v -> v.key() + "::" + e.getKey()))
                        .toList();
                    if (!destinations.isEmpty()) {
                      Pair<List<String>, String> result = Pair.of(destinations, message);
                      context.forward(record.withValue(result));
                    }
                  }
                };
              }
            }, stateStoreName).
        flatMapValues(
            (ValueMapper<Pair<List<String>, String>, Iterable<String>>) value -> value.getLeft()
                .stream().map(v -> v + "::" + value.getRight()).toList()).
        to(topicNameExtractor);

    Topology topology = streamsBuilder.build();
    KafkaStreams kafkaStreams = new KafkaStreams(topology,
        streamsConfig);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, NODES_INPUT_TOPIC,
        NODES_OP_STATUS_OUT, MESSAGES_INPUT_TOPIC);
    await().atMost(Duration.ofSeconds(120))
        .until(() -> kafkaStreams.state().equals(KafkaStreams.State.RUNNING));

    //Produce 1 person
    NodeInputPojo inputPojo = NodeInputPojo.builder().opType(OpType.CREATE_PERSON)
        .createPersonOp(new CreatePersonOp(new PersonPojo("user1", "some dude", 2)))
        .build();
    commonTestUtils.produceSingleEvent(NODES_INPUT_TOPIC,
        inputPojo.getCreatePersonOp().getPersonPojo().getUsername(), inputPojo,
        new Header[0], StringSerializer.class, nodeInputJsonSerde.serializer().getClass());

    //Produce 2nd person
    NodeInputPojo inputPojo2 = NodeInputPojo.builder().opType(OpType.CREATE_PERSON)
        .createPersonOp(new CreatePersonOp(new PersonPojo("user2", "some other dude", 2)))
        .build();
    commonTestUtils.produceSingleEvent(NODES_INPUT_TOPIC,
        inputPojo2.getCreatePersonOp().getPersonPojo().getUsername(), inputPojo2,
        new Header[0], StringSerializer.class, nodeInputJsonSerde.serializer().getClass());

    //Create relationship
    NodeInputPojo inputPojo3 = NodeInputPojo.builder().opType(OpType.FOLLOW_PERSON)
        .followPersonOp(new FollowPersonOp("user1", "user2")).build();
    commonTestUtils.produceSingleEvent(NODES_INPUT_TOPIC,
        inputPojo3.getFollowPersonOp().getUsername(), inputPojo3,
        new Header[0], StringSerializer.class, nodeInputJsonSerde.serializer().getClass());

    //Expecting to consume 3 events on the output topic
    commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
        NODES_OP_STATUS_OUT, 3);
    Neo4jStore store = storeSupplier.neo4jStore;
    TraversalResult traversalResult = store.findRelatedNodes("user1",
        new TraversalParametersBuilder().withTraversalOrder(TraversalOrder.BREADTH_FIRST)
            .withMaxDepth(searchDepth)
            .addRelationshipToTraverse(relationshipType, RelationshipTraversalDirection.OUT)
            .build());
    log.info("Result : {}", traversalResult);
    commonTestUtils.produceSingleEvent(MESSAGES_INPUT_TOPIC, "user1",
        "Message about something exciting", new Header[0], StringSerializer.class,
        StringSerializer.class);

    commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
        "user2", 1);
    //Verify state store content
    NodePojo node1 = store.getNodeByKey("user1");

    assertThat(node1.key()).isEqualTo("user1");

    assertThat(node1.properties()).containsExactlyInAnyOrderEntriesOf(
        Map.of("fullname", "some dude", "defaultFollowerDepth", "2"));

    List<RelationshipPojo> relationshipPojoList = store.getNodeRelationships("user2");

    assertThat(relationshipPojoList).hasSize(1);
    assertThat(relationshipPojoList.get(0).endNodeKey).isEqualTo("user2");
    assertThat(relationshipPojoList.get(0).direction).isEqualTo(Direction.IN);
    assertThat(relationshipPojoList.get(0).startNodeKey).isEqualTo("user1");

    streamsLatch.countDown();
  }
}

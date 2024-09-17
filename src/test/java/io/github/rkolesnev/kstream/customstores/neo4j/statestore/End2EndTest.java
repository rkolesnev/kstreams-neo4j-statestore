package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import static io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.KStreamApp.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters.RelationshipTraversalDirection;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters.TraversalOrder;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters.TraversalParametersBuilder;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.KStreamApp;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.MessageToPersonJoinProcessor;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.PersonNodeProcessor;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.*;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.NodeInputPojo.OpType;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.RelationshipPojo.Direction;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.RelationshipType;

@Slf4j
public class End2EndTest {


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

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Properties streamsConfig = commonTestUtils.getPropertiesForStreams();

        TestNeo4JStoreSupplier storeSupplier = new TestNeo4JStoreSupplier(STATE_STORE_NAME);

        KStreamApp app = new KStreamApp();
        Topology topology = app.builtKStreamTopology(streamsBuilder,storeSupplier);
        KafkaStreams kafkaStreams = new KafkaStreams(topology,
                streamsConfig);

        commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, NODES_INPUT_TOPIC,
                NODES_OP_STATUS_OUT, MESSAGES_INPUT_TOPIC);
        await().atMost(Duration.ofSeconds(120))
                .until(() -> kafkaStreams.state().equals(KafkaStreams.State.RUNNING));

        //Produce 1 person
        NodeInputPojo inputPojo = NodeInputPojo.builder().opType(OpType.CREATE_PERSON)
                .createPersonOp(new CreatePersonOp(new PersonPojo("user1", "some person")))
                .build();
        commonTestUtils.produceSingleEvent(NODES_INPUT_TOPIC,
                inputPojo.getCreatePersonOp().getPersonPojo().getUsername(), inputPojo,
                new Header[0], StringSerializer.class, nodeInputJsonSerde.serializer().getClass());

        //Produce 2nd person
        NodeInputPojo inputPojo2 = NodeInputPojo.builder().opType(OpType.CREATE_PERSON)
                .createPersonOp(new CreatePersonOp(new PersonPojo("user2", "some other person")))
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

        //Produce 3rd person
        NodeInputPojo inputPerson3 = NodeInputPojo.builder().opType(OpType.CREATE_PERSON)
                .createPersonOp(new CreatePersonOp(new PersonPojo("user3", "another person")))
                .build();
        commonTestUtils.produceSingleEvent(NODES_INPUT_TOPIC,
                inputPerson3.getCreatePersonOp().getPersonPojo().getUsername(), inputPerson3,
                new Header[0], StringSerializer.class, nodeInputJsonSerde.serializer().getClass());

        //Create relationship
        NodeInputPojo inputPojo4 = NodeInputPojo.builder().opType(OpType.FOLLOW_PERSON)
                .followPersonOp(new FollowPersonOp("user2", "user3")).build();
        commonTestUtils.produceSingleEvent(NODES_INPUT_TOPIC,
                inputPojo4.getFollowPersonOp().getUsername(), inputPojo4,
                new Header[0], StringSerializer.class, nodeInputJsonSerde.serializer().getClass());

        //Expecting to consume 5 events on the output topic
        commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
                NODES_OP_STATUS_OUT, 5);

        //Grab state store ref directly from supplier and verify content
        Neo4jStore store = storeSupplier.neo4jStore;
        TraversalResult traversalResult = store.findRelatedNodes("user1",
                new TraversalParametersBuilder().withTraversalOrder(TraversalOrder.BREADTH_FIRST)
                        .withMaxDepth(SEARCH_DEPTH)
                        .addRelationshipToTraverse(RELATIONSHIP_TYPE, RelationshipTraversalDirection.OUT)
                        .build());
        log.info("Result : {}", traversalResult);
        assertThat(traversalResult.getFoundNodesWithLevel()).containsExactlyInAnyOrderEntriesOf(Map.of(1, Set.of(new NodePojo("user2", Map.of("fullname", "some other person"))),
                2, Set.of(new NodePojo("user3", Map.of( "fullname", "another person")))));

        //Verify state store content
        NodePojo node1 = store.getNodeByKey("user1");

        assertThat(node1.key()).isEqualTo("user1");

        assertThat(node1.properties()).containsExactlyInAnyOrderEntriesOf(
                Map.of("fullname", "some person"));

        List<RelationshipPojo> relationshipPojoList = store.getNodeRelationships("user2");

        assertThat(relationshipPojoList).hasSize(2);
        assertThat(relationshipPojoList.get(0).endNodeKey).isEqualTo("user3");
        assertThat(relationshipPojoList.get(0).direction).isEqualTo(Direction.OUT);
        assertThat(relationshipPojoList.get(0).startNodeKey).isEqualTo("user2");
        assertThat(relationshipPojoList.get(0).relationshipType).isEqualTo(RELATIONSHIP_TYPE.name());


        assertThat(relationshipPojoList.get(1).endNodeKey).isEqualTo("user2");
        assertThat(relationshipPojoList.get(1).direction).isEqualTo(Direction.IN);
        assertThat(relationshipPojoList.get(1).startNodeKey).isEqualTo("user1");
        assertThat(relationshipPojoList.get(1).relationshipType).isEqualTo(RELATIONSHIP_TYPE.name());


        //Test message flow

        commonTestUtils.produceSingleEvent(MESSAGES_INPUT_TOPIC, "user1",
                "Message about something exciting", new Header[0], StringSerializer.class,
                StringSerializer.class);

        List<ConsumerRecord> records = commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
                "user2", 1);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo("user1");
        assertThat((String) records.get(0).value()).contains("Message about something exciting");

        records = commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
                "user3", 1);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo("user1");
        assertThat((String) records.get(0).value()).contains("Message about something exciting");

        //Pause to allow exploration in Neo4jBrowser..
        //await().atMost(Duration.ofSeconds(1200)).until(()->false);
        streamsLatch.countDown();
    }
}

package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp;

import io.github.rkolesnev.kstream.customstores.neo4j.statestore.Neo4JStoreBuilder;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.Neo4jStoreSupplier;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.DestinationMessage;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.DestinationMessageJsonSerde;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.NodeInputJsonSerde;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.neo4j.graphdb.RelationshipType;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KStreamApp {

    public static final String NODES_INPUT_TOPIC = "NODES_IN";
    public static final String MESSAGES_INPUT_TOPIC = "MSG_IN";
    public static final String NODES_OP_STATUS_OUT = "NODES_OP_STATUS_OUT";
    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("Follows");
    public static final String STATE_STORE_NAME = "Neo4jStoreForDemo";
    public static final int SEARCH_DEPTH = 3;

    public static void main(String[] args) {
        CountDownLatch streamsLatch = new CountDownLatch(1);
        KStreamApp app = new KStreamApp();
        Runtime.getRuntime().addShutdownHook(new Thread(streamsLatch::countDown));
        app.startKStreams(streamsLatch);
    }

    @SneakyThrows
    public void startKStreams(CountDownLatch streamsLatch) {
        createTopics();
        Properties streamsConfig = getStreamsConfig();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Neo4jStoreSupplier storeSupplier = new Neo4jStoreSupplier(STATE_STORE_NAME);
        Topology topology = builtKStreamTopology(streamsBuilder, storeSupplier);

        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig)) {
            kafkaStreams.start();
            streamsLatch.await();
        }

    }

    @SneakyThrows
    private void createTopics(){
        AdminClient adminClient = KafkaAdminClient.create(
                getStreamsConfig());
        Set<String> existingTopics = adminClient.listTopics().names().get();
        adminClient.createTopics(Stream.of(NODES_INPUT_TOPIC,MESSAGES_INPUT_TOPIC).filter(topic->!existingTopics.contains(topic)).map(topic ->
                new NewTopic(topic, 1, (short) 1)).collect(Collectors.toList()));
    }

    private Properties getStreamsConfig() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //Make sure it matches your docker setupssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kstream-app-" + UUID.randomUUID());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // disable ktable cache
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }


    public Topology builtKStreamTopology(StreamsBuilder streamsBuilder, Neo4jStoreSupplier storeSupplier) {
        NodeInputJsonSerde nodeInputJsonSerde = new NodeInputJsonSerde();
        DestinationMessageJsonSerde destinationMessageJsonSerde = new DestinationMessageJsonSerde();

        streamsBuilder.addStateStore(new Neo4JStoreBuilder(storeSupplier));
        streamsBuilder.stream(NODES_INPUT_TOPIC,
                        Consumed.with(new Serdes.StringSerde(), nodeInputJsonSerde))
                .process(
                        () -> new PersonNodeProcessor(STATE_STORE_NAME, RELATIONSHIP_TYPE), STATE_STORE_NAME)
                .to(NODES_OP_STATUS_OUT);
        TopicNameExtractor<String, DestinationMessage> topicNameExtractor = (k, v, recordContext) -> v.getDestinationUser();
        streamsBuilder.stream(MESSAGES_INPUT_TOPIC, Consumed.with(new Serdes.StringSerde(), new Serdes.StringSerde()))
                .processValues(
                        () -> new MessageToPersonJoinProcessor(STATE_STORE_NAME, SEARCH_DEPTH, RELATIONSHIP_TYPE), STATE_STORE_NAME).
                flatMapValues(
                        (ValueMapper<List<DestinationMessage>, Iterable<DestinationMessage>>) destinationMessages -> destinationMessages)
                .to(topicNameExtractor, Produced.with(new Serdes.StringSerde(), destinationMessageJsonSerde));

        return streamsBuilder.build();
    }

}

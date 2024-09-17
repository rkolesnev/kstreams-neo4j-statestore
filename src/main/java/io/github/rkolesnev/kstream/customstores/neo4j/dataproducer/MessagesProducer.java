package io.github.rkolesnev.kstream.customstores.neo4j.dataproducer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.rkolesnev.kstream.customstores.neo4j.dataproducer.PersonDataProducer.PERSONS_DATA;
import static io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.KStreamApp.MESSAGES_INPUT_TOPIC;

@Slf4j
public class MessagesProducer {
    static long sendRandomSeed = 123456L;

    @SneakyThrows
    public static void main(String[] args) {
        Properties props = getProducerProperties();
        int personsNumber = 40;
        long sendIntervalSeconds = 5;
        CountDownLatch produceLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(produceLatch::countDown));
        AtomicBoolean running = new AtomicBoolean(true);
        new Thread(() -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                Random messageSendRandom = new Random(sendRandomSeed);
                while (running.get()) {
                    producer.send(getRandomMessageToSend(messageSendRandom.nextInt(personsNumber)));
                    producer.flush();
                    try {
                        Thread.sleep(sendIntervalSeconds * 1000);
                    }catch (InterruptedException e){
                        throw new RuntimeException(e);
                    }
                }
            }
        }).start();
        produceLatch.await();
        running.set(false);
    }

    private static ProducerRecord<String, String> getRandomMessageToSend(int senderIndex) {
        String sender = PERSONS_DATA[senderIndex].getLeft();
        String message = "Message from " + sender + ". Hey everybody!";
        log.info("Sending {}", message);
        return new ProducerRecord<>(MESSAGES_INPUT_TOPIC, sender, message);
    }

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "persons-producer-" + UUID.randomUUID());
        return props;
    }
}

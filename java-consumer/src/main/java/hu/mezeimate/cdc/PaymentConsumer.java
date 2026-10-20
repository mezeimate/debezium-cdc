package hu.mezeimate.cdc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class PaymentConsumer {
    private static final String BROKER = "localhost:9092";
    private static final String TOPIC = "bank-cdc.bank.PAYMENT";
    private static final String GROUP = "cdc-lab-java-consumer";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(PaymentConsumer.class);

    public static void main(String[] args) throws JsonProcessingException {
        KafkaConsumer<String, String> consumer = getStringStringKafkaConsumer();
        AtomicBoolean running = new AtomicBoolean(true);
        Thread consumerThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (running.getAndSet(false)) {
                consumer.wakeup();
            }
            try {
                consumerThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "consumer-shutdown"));

        LOGGER.info("Kafka: {} | Topic: {} | Group: {}", BROKER, TOPIC, GROUP);
        LOGGER.info("Waiting for CDC events. Stop with Ctrl+C.");

        try {
            consumer.subscribe(List.of(TOPIC));
            while (running.get()) {
                var consumerRecords = consumer.poll(Duration.ofSeconds(1));
                for (var consumerRecord : consumerRecords) {
                    printRecord(consumerRecord);
                }
                if (!consumerRecords.isEmpty()) {
                    // Kafka stores the group's next offset after the entire batch is printed.
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } finally {
            running.set(false);
            consumer.close(Duration.ofSeconds(5));
            LOGGER.info("Consumer stopped.");
        }
    }

    private static KafkaConsumer<String, String> getStringStringKafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaConsumer<>(properties);
    }

    private static void printRecord(ConsumerRecord<String, String> consumerRecord) throws JsonProcessingException {
        LOGGER.info("--------------------------------------------------");
        LOGGER.info("Partition: {}", consumerRecord.partition());
        LOGGER.info("Offset: {}", consumerRecord.offset());
        LOGGER.info("Key: {}", consumerRecord.key());

        if (consumerRecord.value() == null) {
            LOGGER.info("Operation: TOMBSTONE");
            LOGGER.info("Before/After: not present (null Kafka value)");
            return;
        }

        JsonNode event = JSON.readTree(consumerRecord.value());
        String op = event.path("op").asText("?");
        String operation = switch (op) {
            case "r" -> "SNAPSHOT";
            case "c" -> "INSERT";
            case "u" -> "UPDATE";
            case "d" -> "DELETE";
            default -> "UNKNOWN";
        };
        LOGGER.info("Operation: {} (op={})", operation, op);
        JsonNode before = event.path("before");
        JsonNode after = event.path("after");
        JsonNode payment = after.isObject() ? after : before;
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Payment ID: {}", payment.path("id").asText("unknown"));
        }
        printRow("Before", before);
        printRow("After", after);
    }

    private static void printRow(String label, JsonNode row) {
        if (row.isNull() || row.isMissingNode()) {
            LOGGER.info("{}: null", label);
            return;
        }
        LOGGER.info("{}:", label);
        row.properties().forEach(field ->
                LOGGER.info("  {}: {}", field.getKey(), field.getValue().asText("null"))
        );
    }
}

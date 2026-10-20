package hu.mezeimate.cdc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class PaymentConsumer {
    private static final String BROKER = "localhost:9092";
    private static final String TOPIC = "bank-cdc.bank.PAYMENT";
    private static final String GROUP = "cdc-lab-java-consumer";
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws JsonProcessingException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
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

        System.out.printf("Kafka: %s | Topic: %s | Group: %s%n", BROKER, TOPIC, GROUP);
        System.out.println("Waiting for CDC events. Stop with Ctrl+C.");

        try {
            consumer.subscribe(List.of(TOPIC));
            while (running.get()) {
                var records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    printRecord(record);
                }
                if (!records.isEmpty()) {
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
            System.out.println("Consumer stopped.");
        }
    }

    private static void printRecord(ConsumerRecord<String, String> record) throws JsonProcessingException {
        System.out.println("--------------------------------------------------");
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());

        if (record.value() == null) {
            System.out.println("Operation: TOMBSTONE");
            System.out.println("Before/After: not present (null Kafka value)");
            return;
        }

        JsonNode event = JSON.readTree(record.value());
        String op = event.path("op").asText("?");
        String operation = switch (op) {
            case "r" -> "SNAPSHOT";
            case "c" -> "INSERT";
            case "u" -> "UPDATE";
            case "d" -> "DELETE";
            default -> "UNKNOWN";
        };
        System.out.printf("Operation: %s (op=%s)%n", operation, op);
        JsonNode before = event.path("before");
        JsonNode after = event.path("after");
        JsonNode payment = after.isObject() ? after : before;
        System.out.println("Payment ID: " + payment.path("id").asText("unknown"));
        printRow("Before", before);
        printRow("After", after);
    }

    private static void printRow(String label, JsonNode row) {
        if (row.isNull() || row.isMissingNode()) {
            System.out.println(label + ": null");
            return;
        }
        System.out.println(label + ":");
        row.fields().forEachRemaining(field ->
                System.out.printf("  %s: %s%n", field.getKey(), field.getValue().asText("null")));
    }
}

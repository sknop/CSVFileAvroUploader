package io.confluent.bootcamp;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import testcontainers.SchemaRegistryContainer;

import java.io.File;
import static org.junit.jupiter.api.Assertions.fail;

class CSVFileAvroUploaderFullTest {
    private final CSVFileAvroUploader uploader = new CSVFileAvroUploader();

    public static Network network = Network.newNetwork();

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"))
            .withNetwork(network);

    @Container
    public static SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.9.0"))
            .withKafka(kafka);

    @BeforeAll
    public static void startUp() {
        kafka.start();
        schemaRegistry.start();
    }

    @AfterAll
    public static void shutdown() {
        schemaRegistry.stop();
        kafka.stop();
    }

    @Test
    void testUploader() {
        File testFile = new File("src/test/resources/test.csv");

        uploader.namespace = "io.confluent.bootcamp.rails.schema";
        uploader.topic = "test-topic";
        uploader.inputFile = testFile.getAbsolutePath();
        uploader.keyField = "canx_reason_code";
        uploader.keyFieldProvided = true;

        uploader.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        uploader.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        uploader.properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        uploader.properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.getSchemaRegistries());

        try {
            uploader.readAndProcessInputFile();
        } catch (Exception e) {
            fail("Should never throw an exception");
        }
    }
}

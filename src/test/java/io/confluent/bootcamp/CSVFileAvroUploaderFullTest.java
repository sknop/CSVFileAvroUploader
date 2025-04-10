package io.confluent.bootcamp;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
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

import java.util.List;
import java.util.Map;

import java.io.*;
import java.nio.file.Files;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

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
        File outputFile = new File("outputfile.json");

        uploader.namespace = "io.confluent.bootcamp.rails.schema";
        uploader.topic = "test-topic";
        uploader.inputFile = testFile.getAbsolutePath();
        uploader.keyField = "toc_id";
        uploader.keyFieldProvided = true;
        uploader.outputFile = outputFile.getAbsolutePath();

        var topic = new NewTopic(uploader.topic, 2, (short) 1);
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(topic));
        }

        uploader.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        uploader.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        uploader.properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        uploader.properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.getSchemaRegistries());

        try {
            uploader.readAndProcessInputFile();
        } catch (Exception e) {
            fail("Should never throw an exception");
        }

        try {
            String line = Files.readString(outputFile.toPath());
            assertEquals("{ \"offset\": {\n\"0\" : \"51\", \"1\" : \"49\"\n} }", line);
        }
        catch (FileNotFoundException e) {
            fail("Cannot find outputFile " + outputFile);
        }
        catch (IOException e) {
            fail("IOException " + e);
        }

        assertTrue(outputFile.delete());
    }
}

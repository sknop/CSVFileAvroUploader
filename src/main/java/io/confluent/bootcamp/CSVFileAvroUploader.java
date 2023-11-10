package io.confluent.bootcamp;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import picocli.CommandLine;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@CommandLine.Command(
        scope = CommandLine.ScopeType.INHERIT,
        synopsisHeading = "%nUsage:%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false)
public class CSVFileAvroUploader implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--config-file"}, required = true,
            description = "Kafka configuration file (required)")
    protected String configFile = null;

    @CommandLine.Option(names = {"-f", "--input-file"}, required = true,
            description = "File from which to read (required)")
    private String inputFile = null;

    @CommandLine.Option(names = {"--topic"}, required = true, description = "Topic to write to (required)")
    private String topic;

    @CommandLine.Option(names = {"-k", "--key-field"}, description = "If provided, use this column as the key")
    private String keyField = null;

    private boolean keyFieldProvided = false;

    protected Logger logger = LoggerFactory.getLogger(CSVFileAvroUploader.class);
    private String[] headerEntries;

    private final Properties properties = new Properties();

    public void readConfigFile() {
        if (configFile != null) {
            logger.info("Reading config file " + configFile);

            try (InputStream inputStream = new FileInputStream(configFile)) {

                properties.load(inputStream);
                logger.info(properties.entrySet()
                        .stream()
                        .map(e -> e.getKey() + " : " + e.getValue())
                        .collect(Collectors.joining(", ")));

                properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                if (keyField != null && !keyField.isEmpty()) {
                    keyFieldProvided = true;
                }

                if (keyFieldProvided) {
                    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                }
                else {
                    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
                }
            } catch (FileNotFoundException e) {
                logger.error("Input file {} not found", inputFile);
                System.exit(1);
            } catch (IOException e) {
                logger.error("Encountered IOException.",e);
            }
        }
        else {
            logger.warn("No config file specified");
        }
    }

    private void readAndProcessInputFile() {
        try (InputStream inputStream = new FileInputStream(inputFile);
             Reader reader = new InputStreamReader(inputStream);
             BufferedReader buffered = new BufferedReader(reader)) {

            String header = buffered.readLine();
            String locationSchema = processHeader(header);
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(locationSchema);

            KafkaProducer<Object, Object> producer = new KafkaProducer<>(properties);

            while (buffered.ready()) {
                String line = buffered.readLine();
                var values = line.split(",");

                String key = null;
                GenericRecord avroRecord = new GenericData.Record(schema);
                for (int index = 0; index < values.length; index++) {
                    avroRecord.put(headerEntries[index], values[index]);
                    if (keyField != null && !keyField.isEmpty() && headerEntries[index].equals(keyField)) {
                        key = values[index];
                    }
                }
                
                logger.trace("Key = {}, Record = {}", key, avroRecord);
                ProducerRecord<Object, Object> record = keyFieldProvided ? new ProducerRecord<>(topic, key, avroRecord) :
                        new ProducerRecord<>(topic, avroRecord);
                try {
                    producer.send(record);
                } catch (SerializationException e) {
                    logger.error("This should never happen: ", e);
                    throw new RuntimeException(e);
                }
            }
            producer.flush();
            producer.close();

        }catch (FileNotFoundException e) {
            logger.error("Input file {} not found", inputFile);
            System.exit(1);
        } catch (IOException e) {
            logger.error("Encountered IOException.",e);
        }

    }

    String processHeader(String header) {
        headerEntries = header.split(",");

        StringBuilder locationSchemaBuilder =
                new StringBuilder("{\"type\":\"record\"," +
                                  "\"name\":\"location\"," +
                                  "\"fields\":");
        locationSchemaBuilder.append("[");

        String format = "{\"name\":\"%s\",\"type\":\"string\"}";
        var processedEntries = Arrays.stream(headerEntries).map(entry -> String.format(format, entry)).toArray(String[]::new);
        locationSchemaBuilder.append(String.join(",", processedEntries));

        locationSchemaBuilder.append("]");
        locationSchemaBuilder.append("}");

        return locationSchemaBuilder.toString();
    }

    @Override
    public Integer call() {
        readConfigFile();
        readAndProcessInputFile();

        return 0;
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new CSVFileAvroUploader()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
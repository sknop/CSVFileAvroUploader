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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
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
        sortOptions = false,
        versionProvider = Version.class,
        description = "Upload CSV file in Avro format and create the schema from the first header entry"
        )
public class CSVFileAvroUploader implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--config-file"}, required = true,
            description = "Kafka configuration file (required)")
    protected String configFile = null;

    @CommandLine.Option(names = {"-f", "--input-file"}, required = true,
            description = "File from which to read (required)")
    String inputFile = null;

    @CommandLine.Option(names = {"--topic"}, required = true, description = "Topic to write to (required)")
    String topic;

    @CommandLine.Option(names = {"-k", "--key-field"}, description = "If provided, use this column as the key")
    String keyField = null;

    @CommandLine.Option(names = {"-s","--schema-name"}, required = true, description = "Name of the schema (required")
    String schemaName = null; // package private for testing

    @CommandLine.Option(names = {"-n","--namespace"}, required = true, description = "Name of the namespace (required)")
    String namespace = null;

    @CommandLine.Option(names = {"--separator"}, description = "If provided, use this separator (default \",\")")
    String separator = ",";

    @CommandLine.Option(names = {"-o", "--output-file"}, description = "Output file into which the results are written in JSON format")
    String outputFile = null;

    boolean keyFieldProvided = false;

    static protected Logger logger = LoggerFactory.getLogger(CSVFileAvroUploader.class);
    private String[] headerEntries;

    long lastOffset = 0;

    final Properties properties = new Properties();

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

    public static String[] splitCSVFile(String line) {
        return line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    }

    public String quoteSeparator() {
        return Pattern.quote(separator);
    }

    public static ArrayList<String> customSplitSpecific(String line)
    {
        ArrayList<String> words = new ArrayList<>();
        boolean notInsideComma = true;
        int start =0;
        for(int i=0; i<line.length()-1; i++)
        {
            if(line.charAt(i)==',' && notInsideComma)
            {
                words.add(line.substring(start,i));
                start = i+1;
            }
            else if(line.charAt(i)=='"')
                notInsideComma=!notInsideComma;
        }
        words.add(line.substring(start));
        return words;
    }

    void readAndProcessInputFile() {
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
                var values = line.split(quoteSeparator(),-1);

                String key = null;
                GenericRecord avroRecord = new GenericData.Record(schema);
                for (int index = 0; index < values.length; index++) {
                    logger.trace("Adding value {} for index {}", values[index], index);
                    String value = values[index] == null ? "" : values[index];
                    avroRecord.put(headerEntries[index], value);
                    if (keyField != null && !keyField.isEmpty() && headerEntries[index].equals(keyField)) {
                        key = value;
                    }
                }

                logger.trace("Key = {}, Record = {}", key, avroRecord);
                ProducerRecord<Object, Object> record = keyFieldProvided ?
                        new ProducerRecord<>(topic, key, avroRecord) :
                        new ProducerRecord<>(topic, avroRecord);
                try {
                    producer.send(record, (recordMetadata, e) -> {
                        if (e != null) {
                            logger.error("Failed to produce message", e);
                        }
                        else {
                            lastOffset = recordMetadata.offset();

                            logger.info("Produced {} at offset {} in partition {}", record, recordMetadata.offset(), recordMetadata.partition());
                        }
                    });
                } catch (SerializationException e) {
                    logger.error("This should never happen: ", e);
                    throw new RuntimeException(e);
                }
            }
            producer.flush();

            producer.close();

            if (outputFile != null) {
                try(FileWriter fileWriter = new FileWriter(outputFile)) {
                    fileWriter.write(String.format("{ \"offset\": \"%d\" }", lastOffset));
                }
            }
        }catch (FileNotFoundException e) {
            logger.error("Input file {} not found", inputFile);
            System.exit(1);
        } catch (IOException e) {
            logger.error("Encountered IOException.",e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    String processHeader(String header) {
        headerEntries = header.split(quoteSeparator());

        StringBuilder schemaBuilder =
                new StringBuilder("{\"type\":\"record\"," +
                                  "\"name\":\"" + schemaName + "\"," +
                                  "\"namespace\":\"" + namespace + "\"," +
                                  "\"fields\":");
        schemaBuilder.append("[");

        String format = "{\"name\":\"%s\",\"type\":\"string\"}";
        var processedEntries = Arrays.stream(headerEntries).map(entry -> String.format(format, entry)).toArray(String[]::new);
        schemaBuilder.append(String.join(",", processedEntries));

        schemaBuilder.append("]");
        schemaBuilder.append("}");

        return schemaBuilder.toString();
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
            logger.error("Unexpected error", e);
            System.exit(1);
        }
    }

}
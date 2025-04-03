package io.confluent.bootcamp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CSVFileAvroUploaderBasicTest {

    private final CSVFileAvroUploader uploader = new CSVFileAvroUploader();

    @Test
    public void testProcessHeader() {
        String header = "header1|header2|header3";
        uploader.separator = "|";
        uploader.schemaName = "location";
        uploader.namespace = "io.confluent.bootcamp.rails.schema";

        String schema = uploader.processHeader(header);
        String expected = "{\"type\":\"record\",\"name\":\"location\",\"namespace\":\"io.confluent.bootcamp.rails.schema\",\"fields\":[{\"name\":\"header1\",\"type\":\"string\"},{\"name\":\"header2\",\"type\":\"string\"},{\"name\":\"header3\",\"type\":\"string\"}]}";

        assertEquals(expected, schema);
    }

    @Test
    void testSplitCSVFile() {
        String line = "Sachin,,M,\"Maths,Science,English\",Need to improve in these subjects.";

        var splitted1 = CSVFileAvroUploader.splitCSVFile(line);
        var splitted2 = CSVFileAvroUploader.customSplitSpecific(line).toArray(new String[0]);

        assertArrayEquals(splitted1, splitted2);
    }
}
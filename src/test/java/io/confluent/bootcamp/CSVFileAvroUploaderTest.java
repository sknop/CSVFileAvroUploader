package io.confluent.bootcamp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CSVFileAvroUploaderTest {

    private final CSVFileAvroUploader uploader = new CSVFileAvroUploader();

    @Test
    public void testProcessHeader() {
        String header = "header1,header2,header3";

        String schema = uploader.processHeader(header);
        String expected = "{\"type\":\"record\",\"name\":\"location\",\"fields\":[{\"name\":\"header1\",\"type\":\"string\"},{\"name\":\"header2\",\"type\":\"string\"},{\"name\":\"header3\",\"type\":\"string\"}]}";
        assertEquals(schema, expected);
    }
}
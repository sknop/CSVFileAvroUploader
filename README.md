# CSVFileAvroUploader
Kafka Producer that reads from a CSV file and produces the messages in Avro

The first line of the CSV file (the "header") will be used to determine the schema.
Each field will take its name from the corresponding header entry with the type of "string".

Usage:
io.confluent.bootcamp.CSVFileAvroUploader [-hV] -c=<configFile> -f=<inputFile> [-k=<keyField>]
             --topic=<topic>

Description:


Options:

      -c, --config-file=<configFile>
                            Kafka configuration file (required)
      -f, --input-file=<inputFile>
                            File from which to read
          --topic=<topic>   Topic to write to
      -k, --key-field=<keyField>
                            If provided, use this column as the key
      -h, --help            Show this help message and exit.
      -V, --version         Print version information and exit.

  Set logging to "trace" to see every message being produced.
  

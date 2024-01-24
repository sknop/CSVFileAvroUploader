package io.confluent.bootcamp;

import picocli.CommandLine;

public class Version implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() throws Exception {
        Package mainPackage = CSVFileAvroUploader.class.getPackage();
        String version = mainPackage.getImplementationVersion();

        return new String[] { version };
    }
}

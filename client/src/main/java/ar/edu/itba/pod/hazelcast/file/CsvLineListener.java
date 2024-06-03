package ar.edu.itba.pod.hazelcast.file;

@FunctionalInterface
public interface CsvLineListener {
    void onCsvParsedLine(String[] line);
}
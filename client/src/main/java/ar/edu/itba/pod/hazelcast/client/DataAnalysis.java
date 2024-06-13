package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.client.queries.QueryFiveClient;
import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.file.CsvFileWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataAnalysis {

    private final static String ANALYSIS_CSV_PATH = "analysisPath";
    private final static String NODES = "nodes";
    private final static String HEADER = "nodes;query;i;loading;query";
    private final static Integer ITERATIONS = 10;

    public static void main(String[] args) throws InterruptedException {

        List<String[]> rows = new ArrayList<>();
        rows.add(HEADER.split(";"));

        for (int i = 1; i <= ITERATIONS; i++) {
            QueryFiveClient five = new QueryFiveClient();
            Map<String,Double> performanceData = five.getPerformanceRows();

            String result = System.getProperty(NODES) + ";" + "5" + ";" + i + ";" + performanceData.get("loading") + ";" + performanceData.get("query");
            rows.add(result.split(";"));
        }
        CsvFileWriter.writeRows(System.getProperty(ANALYSIS_CSV_PATH)+ "/analysis_query5", rows);

        System.exit(0);
    }
}

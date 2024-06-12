package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.file.CsvFileWriter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class QueryThreeClient extends QueryClient<String, Double> {

    private static final String HEADER = "Issuing Agency;Percentage";

    public QueryThreeClient() {
        super(List.of(ArgumentUtils.N_AGENCIES));
        CsvFileWriter.writeRows(System.getProperty(ArgumentUtils.OUT_PATH) + "/time3.txt", rows);
        System.exit(0);

    }

    public static void main(String[] args) {
        QueryClient<String, Double> client = new QueryThreeClient();
    }

    @Override
    public void loadData(String dirPath) {
        getCityData().getCsvLoader().loadQueryThree(getHazelcastInstance(), dirPath);
    }

    @Override
    public Map<String, Double> solveQuery() {
        return getCityData().getQuerySolver().solveQueryThree(getHazelcastInstance());
    }

    @Override
    public void writeResults(Map<String, Double> resultMap) {
        if (resultMap == null)
            throw new IllegalStateException("Query not executed");

        final Comparator<Map.Entry<String, Double>> valueComparator =
                Map.Entry.<String, Double>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey());

        int limit = Integer.parseInt(System.getProperty(ArgumentUtils.N_AGENCIES));

        final List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"Issuing Agency", "Percentage"});

        System.out.println(HEADER);
        resultMap.entrySet()
                .stream()
                .sorted(valueComparator)
                .limit(limit)
                .forEach(entry -> {
                    System.out.printf("%s;%.2f%%\n", entry.getKey(), entry.getValue() * 100);
                    rows.add(new String[]{entry.getKey(), String.format("%.2f%%", entry.getValue() * 100)});
                });

        String outFile = System.getProperty(ArgumentUtils.OUT_PATH) + "/query3" + ".csv";
        CsvFileWriter.writeRows(outFile, rows);
    }

}

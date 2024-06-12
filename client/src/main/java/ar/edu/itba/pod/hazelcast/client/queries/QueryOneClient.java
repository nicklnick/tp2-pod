package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.file.CsvFileWriter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class QueryOneClient extends QueryClient<String, Integer> {
    public QueryOneClient() {
        super(List.of());
        CsvFileWriter.writeRows(System.getProperty(ArgumentUtils.OUT_PATH) + "/time1.txt", rows);
        System.exit(0);

    }

    public static void main(String[] args) {
        QueryClient<String, Integer> client = new QueryOneClient();
    }

    @Override
    public void loadData(String dirPath) {
        getCityData().getCsvLoader().loadQueryOne(getHazelcastInstance(), dirPath);
    }

    @Override
    public Map<String, Integer> solveQuery() {
        return getCityData().getQuerySolver().solveQueryOne(getHazelcastInstance());
    }

    private static final String HEADER = "Infraction;Tickets";

    @Override
    public void writeResults(Map<String, Integer> resultMap) {
        if (resultMap == null)
            throw new IllegalStateException("Query not executed");

        final Comparator<Map.Entry<String, Integer>> valueComparator =
                Map.Entry.<String, Integer>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey());

        final List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"Infraction", "Tickets"});

        System.out.println(HEADER);
        resultMap.entrySet()
                .stream()
                .sorted(valueComparator)
                .forEach(entry -> {
                    System.out.println(entry.getKey() + ";" + entry.getValue());
                    rows.add(new String[]{entry.getKey(), entry.getValue().toString()});
                });

        String outFile = System.getProperty(ArgumentUtils.OUT_PATH) + "/query1" + ".csv";
        CsvFileWriter.writeRows(outFile, rows);
    }
}

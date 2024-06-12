package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.file.CsvFileWriter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class QueryFourClient extends QueryClient<String, Map.Entry<String, Integer>>  {

    private static final String HEADER = "County;Plate;Tickets";
    public QueryFourClient() {
        super(List.of(ArgumentUtils.FROM_DATE, ArgumentUtils.TO_DATE));
        CsvFileWriter.writeRows(System.getProperty(ArgumentUtils.OUT_PATH) + "/time4.txt", rows);
        System.exit(0);
    }

    public static void main(String[] args) {
        QueryClient<String, Map.Entry<String, Integer>> client = new QueryFourClient();
    }

    @Override
    public void loadData(String dirPath) {
        getCityData().getCsvLoader().loadQueryFour(getHazelcastInstance(), dirPath);
    }

    @Override
    public Map<String, Map.Entry<String, Integer>> solveQuery() {
        return getCityData().getQuerySolver().solveQueryFour(getHazelcastInstance());
    }

    @Override
    public void writeResults(Map<String, Map.Entry<String, Integer>> resultMap) {
        if (resultMap == null)
            throw new IllegalStateException("Query not executed");

        final List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"County", "Plate", "Tickets"});

        System.out.println(HEADER);
        resultMap.entrySet()
                 .stream()
                 .sorted(Map.Entry.comparingByKey())
                 .forEach(entry -> {
                     System.out.println(entry.getKey() + ";" + entry.getValue().getKey() + ";" + entry.getValue().getValue());
                        rows.add(new String[]{entry.getKey(), entry.getValue().getKey(), entry.getValue().getValue().toString()});
                 });

        String outFile = System.getProperty(ArgumentUtils.OUT_PATH) + "/query4" + ".csv";
        CsvFileWriter.writeRows(outFile, rows);
    }
}

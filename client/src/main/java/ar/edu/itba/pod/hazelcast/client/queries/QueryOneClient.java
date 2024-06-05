package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class QueryOneClient extends QueryClient<String, Integer> {
    public QueryOneClient() {
        super(List.of());
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

        System.out.println(HEADER);
        resultMap.entrySet()
                .stream()
                .sorted(valueComparator)
                .forEach(entry -> System.out.println(entry.getKey() + ";" + entry.getValue()));
    }
}

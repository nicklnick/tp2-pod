package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import com.hazelcast.core.HazelcastInstance;

import java.util.*;

public class QueryFiveClient extends QueryClient<String, Integer> {

    public QueryFiveClient() {
        super(List.of());
    }

    public static void main(String[] args) {
        QueryClient<String, Integer> client = new QueryFiveClient();
    }

    @Override
    public void loadData(String path) {
        getCityData().getCsvLoader().loadQueryFive(getHazelcastInstance(), path);
    }

    @Override
    public Map<String, Integer> solveQuery() {
        return getCityData().getQuerySolver().solveQueryFive(getHazelcastInstance());
    }

    @Override
    public void writeResults(Map<String, Integer> resultMap) {
        Map<Integer, Set<String>> hundredGroups = new HashMap<>();
        resultMap.entrySet().forEach(
            entry -> {
                Integer group = entry.getValue() / 100;
                hundredGroups.putIfAbsent(group, new TreeSet<>());
                hundredGroups.get(group).add(entry.getKey());
            }
        );

        hundredGroups.keySet().stream().sorted(Comparator.reverseOrder()).forEach(group -> {
                String[] auxArray = hundredGroups.get(group).toArray(new String[0]);

                for (int current = 0; current < auxArray.length - 1; current++) {
                    for (int j = current + 1; j < auxArray.length; j++) {
                        System.out.println(group + ";" + auxArray[current] + ";" + auxArray[j]);
                    }
                }
            }
        );
    }
}

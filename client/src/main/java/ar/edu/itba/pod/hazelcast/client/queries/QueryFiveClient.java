package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.file.CsvFileWriter;
import com.hazelcast.core.HazelcastInstance;

import java.util.*;

public class QueryFiveClient extends QueryClient<String, Integer> {

    public QueryFiveClient() {
        super(List.of());
        CsvFileWriter.writeRows(System.getProperty(ArgumentUtils.OUT_PATH) + "/time5.txt", rows);
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

    private static final String HEADER = "Group;Infraction A;Infraction B";

    @Override
    public void writeResults(Map<String, Integer> resultMap) {
        final Map<Integer, Set<String>> hundredGroups = new HashMap<>();

        System.out.println(HEADER);
        resultMap.entrySet().forEach(
            entry -> {
                final Integer group = entry.getValue() / 100;
                if(group == 0) return;
                hundredGroups.putIfAbsent(group, new TreeSet<>());
                hundredGroups.get(group).add(entry.getKey());
            }
        );
        final List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"Group", "Infraction A", "Infraction B"});
        System.out.println(HEADER);
        hundredGroups.keySet().stream().sorted(Comparator.reverseOrder()).forEach(group -> {
                final String[] auxArray = hundredGroups.get(group).toArray(new String[0]);
                for (int current = 0; current < auxArray.length - 1; current++) {
                    for (int j = current + 1; j < auxArray.length; j++) {
                        rows.add(new String[]{group * 100 + "", auxArray[current], auxArray[j]});
                        System.out.println(group * 100 + ";" + auxArray[current] + ";" + auxArray[j]);
                    }
                }

            }
        );
        String outFile = System.getProperty(ArgumentUtils.OUT_PATH) + "/query5" + ".csv";
        CsvFileWriter.writeRows(outFile, rows);
        writePerformance(System.getProperty(ArgumentUtils.OUT_PATH) + "/time5" + ".txt");
    }
}

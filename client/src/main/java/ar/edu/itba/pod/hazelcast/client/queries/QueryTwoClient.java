package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.file.CsvFileWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryTwoClient extends QueryClient<String, List<String>> {
    public QueryTwoClient() {
        super(List.of());
        CsvFileWriter.writeRows(System.getProperty(ArgumentUtils.OUT_PATH) + "/time2.txt", rows);
        System.exit(0);

    }

    public static void main(String[] args) {
        QueryClient<String, List<String>> client = new QueryTwoClient();
    }

    @Override
    public void loadData(String dirPath) {
        getCityData().getCsvLoader().loadQueryTwo(getHazelcastInstance(), dirPath);
    }

    @Override
    public Map<String, List<String>> solveQuery() {
        return getCityData().getQuerySolver().solveQueryTwo(getHazelcastInstance());
    }

    private static final String HEADER = "County;InfractionTop1;InfractionTop2;InfractionTop3";

    @Override
    public void writeResults(Map<String, List<String>> resultMap) {
        if (resultMap == null)
            throw new IllegalStateException("Query not executed");

        final List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"County", "InfractionTop1", "InfractionTop2", "InfractionTop3"});

        System.out.println(HEADER);
        resultMap.forEach((county, infractions) -> {
            String infractionTop1 = !infractions.isEmpty() ? infractions.get(0) : "";
            String infractionTop2 = infractions.size() > 1 ? infractions.get(1) : "";
            String infractionTop3 = infractions.size() > 2 ? infractions.get(2) : "";

            System.out.printf("%s;%s;%s;%s%n",
                    county,
                    infractionTop1,
                    infractionTop2,
                    infractionTop3);
            rows.add(new String[]{county, infractionTop1, infractionTop2, infractionTop3});
        });
        String outFile = System.getProperty(ArgumentUtils.OUT_PATH) + "/query2" + ".csv";
        CsvFileWriter.writeRows(outFile, rows);
    }
}

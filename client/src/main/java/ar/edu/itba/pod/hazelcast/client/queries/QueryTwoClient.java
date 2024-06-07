package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;

import java.util.List;
import java.util.Map;

public class QueryTwoClient extends QueryClient<String, List<String>> {
    public QueryTwoClient() {
        super(List.of());
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
        });
    }
}

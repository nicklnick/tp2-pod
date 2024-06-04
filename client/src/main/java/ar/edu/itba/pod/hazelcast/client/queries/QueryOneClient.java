package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import ar.edu.itba.pod.hazelcast.file.CsvFileReader;
import ar.edu.itba.pod.hazelcast.file.util.FileUtils;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionAmountCombinerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionAmountReducerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionCodeMapper;
import ar.edu.itba.pod.hazelcast.models.InfractionNyc;
import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class QueryOneClient extends QueryClient<Integer, Integer> {
    public QueryOneClient() {
        super(List.of());
    }

    public static void main(String[] args) {
        QueryClient<Integer, Integer> client = new QueryOneClient();
    }

    @Override
    public void loadData(String dirPath) {
        // get infractionsXXX.csv and ticketsXXX.csv
        final IMap<Integer, String> map = getHazelcastInstance().getMap(getCityData().getMapName());
        final String infractions = String.join(FileUtils.DELIMITER, dirPath, getCityData().getInfractionsFile());

        final MultiMap<Integer, TicketNyc> mm = getHazelcastInstance().getMultiMap(CredentialUtils.GROUP_NAME);
        final String tickets = String.join(FileUtils.DELIMITER, dirPath, getCityData().getTicketsFile());

        // read infractions and tickets
        CsvFileReader.readRows(infractions, line -> {
            final InfractionNyc infraction = InfractionNyc.fromInfractionNycCsv(line);
            map.put(infraction.getCode(), infraction.getDescription());
        });

        CsvFileReader.readRows(tickets, line -> {
            final TicketNyc ticket = TicketNyc.fromTicketNycCsv(line);
            mm.put(ticket.getCode(), ticket);
        });
    }

    @SuppressWarnings("deprecation")
    @Override
    public Map<Integer, Integer> solveQuery() {
        final MultiMap<Integer, TicketNyc> mm = getHazelcastInstance().getMultiMap(CredentialUtils.GROUP_NAME);

        final JobTracker jobTracker = getHazelcastInstance().getJobTracker(CredentialUtils.GROUP_NAME);

        final KeyValueSource<Integer, TicketNyc> source = KeyValueSource.fromMultiMap(mm);

        final Job<Integer, TicketNyc> job = jobTracker.newJob(source);

        final ICompletableFuture<Map<Integer, Integer>> future = job
                .mapper(new InfractionCodeMapper())
                .combiner(new InfractionAmountCombinerFactory())
                .reducer(new InfractionAmountReducerFactory())
                .submit();

        // print result
        try {
            return future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void writeResults(Map<Integer, Integer> resultMap) {
        if (resultMap == null)
            throw new IllegalStateException("Query not executed");

        final IMap<Integer, String> map = getHazelcastInstance().getMap(getCityData().getMapName());

        final Comparator<Map.Entry<Integer, Integer>> valueComparator =
                Map.Entry.<Integer, Integer>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey());

        resultMap.entrySet()
                .stream()
                .sorted(valueComparator)
                .forEach(entry -> System.out.println(map.get(entry.getKey()) + ";" + entry.getValue()));
    }
}

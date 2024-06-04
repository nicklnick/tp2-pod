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

public class QueryOneClient extends QueryClient<String, Integer> {
    public QueryOneClient() {
        super(List.of());
    }

    public static void main(String[] args) {
        QueryClient<String, Integer> client = new QueryOneClient();
    }

    @Override
    public void loadData(String dirPath) {
        // get infractionsXXX.csv and ticketsXXX.csv
        final IMap<Integer, String> infractions = getHazelcastInstance().getMap(getCityData().getMapName());
        final String infractionsFile = String.join(FileUtils.DELIMITER, dirPath, getCityData().getInfractionsFile());

        final MultiMap<String, TicketNyc> mm = getHazelcastInstance().getMultiMap(CredentialUtils.GROUP_NAME);
        final String tickets = String.join(FileUtils.DELIMITER, dirPath, getCityData().getTicketsFile());

        // read infractions and tickets
        CsvFileReader.readRows(infractionsFile, line -> {
            final InfractionNyc infraction = InfractionNyc.fromInfractionNycCsv(line);
            infractions.put(infraction.getCode(), infraction.getDescription());
        });

        CsvFileReader.readRows(tickets, line -> {
            final TicketNyc ticket = TicketNyc.fromTicketNycCsv(line);
            mm.put(infractions.get(ticket.getCode()), ticket);
        });
    }

    @SuppressWarnings("deprecation")
    @Override
    public Map<String, Integer> solveQuery() {
        final MultiMap<String, TicketNyc> mm = getHazelcastInstance().getMultiMap(CredentialUtils.GROUP_NAME);

        final JobTracker jobTracker = getHazelcastInstance().getJobTracker(CredentialUtils.GROUP_NAME);

        final KeyValueSource<String, TicketNyc> source = KeyValueSource.fromMultiMap(mm);

        final Job<String, TicketNyc> job = jobTracker.newJob(source);

        final ICompletableFuture<Map<String, Integer>> future = job
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

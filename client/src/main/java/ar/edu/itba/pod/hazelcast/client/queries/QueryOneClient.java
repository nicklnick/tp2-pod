package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import ar.edu.itba.pod.hazelcast.file.CsvFileReader;
import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;

import java.util.List;

public class QueryOneClient extends QueryClient {
    private static final String FILE_NAME = "ticketsNYC.csv";

    private MultiMap<Integer, TicketNyc> mm;

    public QueryOneClient() {
        super(List.of());
    }

    public static void main(String[] args) {
        QueryClient client = new QueryOneClient();
    }

    @Override
    public void loadData(HazelcastInstance hazelcastInstance, String dirPath) {
        mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
        final String filePath = String.join("/", dirPath, FILE_NAME);

        CsvFileReader.readRows(filePath, line -> {
            final TicketNyc ticket = TicketNyc.fromTicketNycCsv(line);
            mm.put(ticket.getCode(), ticket);
        });

        // print mm
        mm.entrySet().forEach(System.out::println);
    }

    @Override
    public void solveQuery(HazelcastInstance hazelcastInstance) {

    }
}

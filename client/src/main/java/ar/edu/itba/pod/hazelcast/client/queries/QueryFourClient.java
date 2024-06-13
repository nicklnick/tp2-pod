package ar.edu.itba.pod.hazelcast.client.queries;

import ar.edu.itba.pod.hazelcast.client.QueryClient;
import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.file.CsvFileWriter;
import ar.edu.itba.pod.hazelcast.models.PlateInfractions;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryFourClient extends QueryClient<String, PlateInfractions>  {

    private static final String HEADER = "County;Plate;Tickets";
    public QueryFourClient() {
        super(List.of(ArgumentUtils.FROM_DATE, ArgumentUtils.TO_DATE));
        CsvFileWriter.writeRows(System.getProperty(ArgumentUtils.OUT_PATH) + "/time4.txt", rows);
        System.exit(0);
    }

    public static void main(String[] args) {
        QueryClient<String, PlateInfractions> client = new QueryFourClient();
    }

    @Override
    public void loadData(String dirPath) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        LocalDate from = LocalDate.parse(System.getProperty(ArgumentUtils.FROM_DATE), formatter);
        LocalDate to = LocalDate.parse(System.getProperty(ArgumentUtils.TO_DATE), formatter);
        getCityData().getCsvLoader().loadQueryFour(getHazelcastInstance(), dirPath, from, to);
    }

    @Override
    public Map<String, PlateInfractions> solveQuery() {
        return getCityData().getQuerySolver().solveQueryFour(getHazelcastInstance());
    }

    @Override
    public void writeResults(Map<String, PlateInfractions> resultMap) {
        if (resultMap == null)
            throw new IllegalStateException("Query not executed");

        final List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"County", "Plate", "Tickets"});

        System.out.println(HEADER);
        resultMap.entrySet()
                 .forEach(entry -> {
                     System.out.println(entry.getKey() + ";" + entry.getValue().getPlate() + ";" + entry.getValue().getInfractions());
                        rows.add(new String[]{entry.getKey(), entry.getValue().getPlate(), String.valueOf(entry.getValue().getInfractions())});
                 });

        String outFile = System.getProperty(ArgumentUtils.OUT_PATH) + "/query4" + ".csv";
        CsvFileWriter.writeRows(outFile, rows);
    }
}

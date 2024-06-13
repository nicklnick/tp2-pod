package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.cities.CityData;
import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.file.CsvFileWriter;
import ar.edu.itba.pod.hazelcast.time.Stopwatch;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class QueryClient<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryClient.class);
    protected final List<String[] > rows = new ArrayList<>();
    protected final Map<String,Double> performanceRows  = new HashMap<>();

    private final List<String> arguments = new ArrayList<>(List.of(
            ArgumentUtils.ADDRESSES,
            ArgumentUtils.IN_PATH,
            ArgumentUtils.OUT_PATH,
            ArgumentUtils.CITY
    ));
    private CityData cityData;
    private HazelcastInstance hazelcastInstance;

    public QueryClient(final List<String> arguments) {
        Stopwatch stopwatch = new Stopwatch();
        double dataLoadingDuration;
        double queryDuration;

        this.arguments.addAll(arguments);

        try {
            checkArguments();


            LOGGER.info("hz-config Client Starting ...");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - hz-config Client Starting ..."});

            this.hazelcastInstance = startClient();
            LOGGER.info("hz-config Client started");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - hz-config Client started"});

            LOGGER.info("Loading data ...");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Loading data ..."});
            stopwatch.start();
            this.cityData = CityData.getCityData(System.getProperty(ArgumentUtils.CITY));
            loadData(System.getProperty(ArgumentUtils.IN_PATH));

            dataLoadingDuration = stopwatch.reset();
            rows.add(new String[]{"Stopwatch Data Loading Duration: " + dataLoadingDuration});
            performanceRows.put("loading" ,dataLoadingDuration);

            LOGGER.info("Finished loading data ...");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Finished loading data ..."});

            LOGGER.info("Executing query ...");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Executing query ..."});
            stopwatch.start();
            final Map<K, V> map = solveQuery();
            LOGGER.info("Query executed");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Query executed"});

            LOGGER.info("Writing results ...");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Writing results ..."});
            writeResults(map);
            queryDuration = stopwatch.stop();
            LOGGER.info("Results written");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Results written"});

            rows.add(new String[]{"Stopwatch Query Duration: " + queryDuration});
            performanceRows.put("query" ,queryDuration);

            LOGGER.info("Clearing data ...");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Clearing data ..."});
            clearData();
            LOGGER.info("Data cleared");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Data cleared"});

            LOGGER.info("Exiting ...");
            rows.add(new String[]{ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + " INFO" + " - Exiting ..."});

            System.out.println(dataLoadingDuration);
            System.out.println(queryDuration);
        } catch (IllegalArgumentException e) {
            // TODO: Better message
            LOGGER.error(e.getMessage());
            System.exit(1);
        }
    }

    public HazelcastInstance startClient() {
        // Client config
        final ClientConfig clientConfig = new ClientConfig();

        // Group config
        final GroupConfig groupConfig = new GroupConfig()
                .setName(CredentialUtils.GROUP_NAME)
                .setPassword(CredentialUtils.GROUP_PASSWORD);
        clientConfig.setGroupConfig(groupConfig);

        // Network config
        final ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        final String[] addresses = System.getProperty(ArgumentUtils.ADDRESSES)
                .replaceAll("^'|'$", "")    // Remove leading and trailing quotes
                .split(";");
        networkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(networkConfig);

        // Start the client
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public void clearData() {
        getHazelcastInstance().getMultiMap(CredentialUtils.GROUP_NAME).clear();
    }

    public HazelcastInstance getHazelcastInstance() {
        if(hazelcastInstance == null)
            throw new IllegalStateException("Hazelcast instance not initialized");

        return hazelcastInstance;
    }

    public CityData getCityData() {
        return cityData;
    }

    public abstract void loadData(String path);

    public abstract Map<K, V> solveQuery();

    public abstract void writeResults(Map<K, V> resultMap);

    private void checkArguments() throws IllegalArgumentException {
        for(String argument : arguments)
            if(System.getProperty(argument) == null)
                throw new IllegalArgumentException("Argument " + argument + " is required");
    }

    protected void writePerformance(String outFile){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
            for (String[] row : rows) {
                String line = row[0];
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String,Double> getPerformanceRows() {
        return performanceRows;
    }
}

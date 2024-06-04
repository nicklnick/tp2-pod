package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.client.util.ArgumentUtils;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class QueryClient<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryClient.class);

    private final List<String> arguments = new ArrayList<>(List.of(
            ArgumentUtils.ADDRESSES,
            ArgumentUtils.IN_PATH,
            ArgumentUtils.OUT_PATH,
            ArgumentUtils.CITY
    ));
    private HazelcastInstance hazelcastInstance;



    public QueryClient(final List<String> arguments) {
        this.arguments.addAll(arguments);

        try {
            checkArguments();

            LOGGER.info("hz-config Client Starting ...");
            this.hazelcastInstance = startClient();
            LOGGER.info("hz-config Client started");

            LOGGER.info("Loading data ...");
            loadData(System.getProperty(ArgumentUtils.IN_PATH));
            LOGGER.info("Finished loading data ...");

            LOGGER.info("Executing query ...");
            final Map<K, V> map = solveQuery();
            LOGGER.info("Query executed");

            LOGGER.info("Writing results ...");
            writeResults(map);
            LOGGER.info("Results written");

            LOGGER.info("Clearing data ...");
            clearData();
            LOGGER.info("Data cleared");

            LOGGER.info("Exiting ...");
            System.exit(0);
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

    public HazelcastInstance getHazelcastInstance() {
        if(hazelcastInstance == null)
            throw new IllegalStateException("Hazelcast instance not initialized");

        return hazelcastInstance;
    }

    public void clearData() {
        getHazelcastInstance().getMultiMap(CredentialUtils.GROUP_NAME).clear();
    }

    public abstract void loadData(String path);

    public abstract Map<K, V> solveQuery();

    public abstract void writeResults(Map<K, V> resultMap);


    private void checkArguments() throws IllegalArgumentException {
        for(String argument : arguments)
            if(System.getProperty(argument) == null)
                throw new IllegalArgumentException("Argument " + argument + " is required");
    }

}

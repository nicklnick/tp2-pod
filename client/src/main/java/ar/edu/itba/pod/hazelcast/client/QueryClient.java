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

public abstract class QueryClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryClient.class);

    private final List<String> arguments = new ArrayList<>(List.of(
            ArgumentUtils.ADDRESSES,
            ArgumentUtils.IN_PATH,
            ArgumentUtils.OUT_PATH,
            ArgumentUtils.CITY
    ));

    public QueryClient(final List<String> arguments) {
        this.arguments.addAll(arguments);

        try {
            checkArguments();

            LOGGER.info("hz-config Client Starting ...");
            final HazelcastInstance hazelcastInstance = startClient();
            LOGGER.info("hz-config Client started");

            LOGGER.info("Loading data ...");
            loadData(hazelcastInstance, System.getProperty(ArgumentUtils.IN_PATH));
            LOGGER.info("Finished loading data ...");

            LOGGER.info("Executing query ...");
            solveQuery(hazelcastInstance);
            LOGGER.info("Query executed");
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

    public abstract void loadData(HazelcastInstance hazelcastInstance, String path);

    public abstract void solveQuery(HazelcastInstance hazelcastInstance);

    private void checkArguments() throws IllegalArgumentException {
        for(String argument : arguments)
            if(System.getProperty(argument) == null)
                throw new IllegalArgumentException("Argument " + argument + " is required");
    }
}

package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException {
        LOGGER.info("hz-config Client Starting ...");

        // Client config
        final ClientConfig clientConfig = new ClientConfig();

        // Group config
        final GroupConfig groupConfig = new GroupConfig()
                .setName(CredentialUtils.GROUP_NAME)
                .setPassword(CredentialUtils.GROUP_PASSWORD);
        clientConfig.setGroupConfig(groupConfig);

        // Network config
        final ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        final String[] addresses = { "192.168.1.51:5701", "169.254.157.198:5701" };
        networkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(networkConfig);

        // Start the client
        HazelcastClient.newHazelcastClient(clientConfig);

        LOGGER.info("hz-config Client started");
    }
}

package ar.edu.itba.pod.hazelcast.server;

import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class Server {
    private final static Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private static final String INTERFACE_MASK = "192.168.1.*";
    private static final String MAN_CENTER_URL = "http://localhost:8080/mancenter/";

    public static void main(String[] args) {
        LOGGER.info("hz-config Server starting ...");

        final Config config = new Config();

        // Group config
        final GroupConfig groupConfig = new GroupConfig()
                .setName(CredentialUtils.GROUP_NAME)
                .setPassword(CredentialUtils.GROUP_PASSWORD);
        config.setGroupConfig(groupConfig);

        // Network config
        final JoinConfig joinConfig = new JoinConfig().setMulticastConfig(new MulticastConfig());

        final InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(Collections.singletonList(INTERFACE_MASK));

        final NetworkConfig networkConfig = new NetworkConfig()
                .setInterfaces(interfacesConfig)
                .setJoin(joinConfig);

        config.setNetworkConfig(networkConfig);

        // Management center config
        ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
                .setEnabled(true)
                .setUrl(MAN_CENTER_URL);
        config.setManagementCenterConfig(managementCenterConfig);

        // Start the server
        final HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);
        LOGGER.info("hz-config Server started");
        LOGGER.info("hz-config Cluster discoverable on: {}", hazelcast.getCluster().getLocalMember().getAddress());
    }
}

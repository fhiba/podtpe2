package ar.edu.itba.pod.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {

        //Mask
        String maybeMask = System.getProperty("mask");
        String mask = maybeMask != null ? maybeMask :"127.0.0.*";
        Collection<String> interfaces = Collections.singletonList(mask);

        //Config
        Config config = new Config();

        //Configure group
        GroupConfig groupConfig = new GroupConfig()
                .setName("g12")
                .setPassword("g12-pass");
        config.setGroupConfig(groupConfig);

        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName("default");
        multiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        config.addMultiMapConfig(multiMapConfig);

        //Network config
        MulticastConfig multicastConfig = new MulticastConfig();
        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);
        InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(interfaces)
                .setEnabled(true);

        NetworkConfig networkConfig = new NetworkConfig()
                .setInterfaces(interfacesConfig)
                .setJoin(joinConfig);

        config.setNetworkConfig(networkConfig);

        // Start cluster
        Hazelcast.newHazelcastInstance(config);
    }

}

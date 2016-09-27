package org.github.etacassiopeia.kafka.connect.example;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

/**
 * <p>
 * Command line utility that runs Kafka Connect in distributed mode. In this mode, the process joints a group of other workers
 * and work is distributed among them. This is useful for running Connect as a service, where connectors can be
 * submitted to the cluster to be automatically executed in a scalable, distributed fashion. This also allows you to
 * easily scale out horizontally, elastically adding or removing capacity simply by starting or stopping worker
 * instances.
 * </p>
 */
@InterfaceStability.Unstable
public class ConnectDistributed {
    private static final Logger log = LoggerFactory.getLogger(ConnectDistributed.class);

    public static void main(String[] args) throws Exception {
//        new JHades().overlappingJarsReport();

        if (args.length < 1) {
            log.info("Usage: ConnectDistributed worker.properties");
            System.exit(1);
        }

        String workerPropsFile = args[0];
        Map<String, String> workerProps = !workerPropsFile.isEmpty() ?
                Utils.propsToStringMap(Utils.loadProps(workerPropsFile)) : Collections.<String, String>emptyMap();

        Time time = new SystemTime();

        workerProps.put(WorkerConfig.REST_PORT_CONFIG, String.valueOf(findRandomOpenPortOnAllLocalInterfaces()));
        DistributedConfig config = new DistributedConfig(workerProps);

        RestServer rest = new RestServer(config);
        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();
        log.info(workerId);

        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(config);

        Worker worker = new Worker(workerId, time, config, offsetBackingStore);

        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, worker.getInternalValueConverter());
        statusBackingStore.configure(config);

        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(worker.getInternalValueConverter());
        configBackingStore.configure(config);

        DistributedHerder herder = new DistributedHerder(config, time, worker, statusBackingStore, configBackingStore,
                advertisedUrl.toString());
        final Connect connect = new Connect(herder, rest);
        try {
            connect.start();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            connect.stop();
        }

        // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
        connect.awaitStop();

    }

    private static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}

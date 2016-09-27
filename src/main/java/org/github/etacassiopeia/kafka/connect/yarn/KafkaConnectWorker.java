package org.github.etacassiopeia.kafka.connect.yarn;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * <h1>TransporterWorker</h1>
 * The TransporterWorker
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 22/08/16
 */
public class KafkaConnectWorker {
    private static final Log LOG = LogFactory.getLog(KafkaConnectWorker.class);

    public static void main(String[] args) {

        final KafkaConnectWorker kafkaConnectWorker = new KafkaConnectWorker();
        if (kafkaConnectWorker.init(args)) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    kafkaConnectWorker.close();
                }
            });
            try {
                kafkaConnectWorker.run(args);
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public void run(String[] args) throws IOException {
        configLog4j();

        String workerPropsFile = args[0];

        LOG.info(">> Loading config file from : " + workerPropsFile);

        Map<String, String> workerProps = null;

        try {
            Path pt = new Path(workerPropsFile);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            Properties props=new Properties();
            props.load(br);

            workerProps = !workerPropsFile.isEmpty() ?
                    Utils.propsToStringMap(props) : Collections.<String, String>emptyMap();

        } catch (Exception e) {
            LOG.error(e);
        }

        Time time = new SystemTime();

        workerProps.put(WorkerConfig.REST_PORT_CONFIG, String.valueOf(findRandomOpenPortOnAllLocalInterfaces()));
        DistributedConfig config = new DistributedConfig(workerProps);

        RestServer rest = new RestServer(config);
        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        LOG.info(" >>> WorkerId : " + workerId);

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
            LOG.error(e.getMessage(), e);
            connect.stop();
        }

        // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
        connect.awaitStop();
    }

    private void configLog4j() {
        InputStream is = null;
        try {
            is = getClass().getResourceAsStream("/log4j.properties");
            Properties originalProperties = new Properties();
            originalProperties.load(is);
            LogManager.resetConfiguration();
            PropertyConfigurator.configure(originalProperties);
        } catch (IOException e) {
            LOG.error(e);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    public void close() {
    }

    public boolean init(String[] args) {

        configLog4j();

        return true;
    }

    private static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

}

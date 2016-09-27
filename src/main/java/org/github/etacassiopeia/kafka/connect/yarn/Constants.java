package org.github.etacassiopeia.kafka.connect.yarn;

/**
 * <h1>Constants</h1>
 * The Constants
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 22/08/16
 */
public class Constants {

    public static final String JOB_ID = "job.id";

    public static final String APP_MASTER_MEMORY = "master.memory.default";
    public static final String APP_MASTER_VCORE = "master.vcores.default";

    public static final String CONTAINER_MEMORY = "container.memory.default";
    public static final String CONTAINER_VCORE = "container.vcores.default";

    public static final String CONFIG_CONTAINER_COUNT = "container";

    public static final String DISTRIBUTED_JAR_LOCATION = "distributedConnectorJarLocation";
    public static final String DISTRIBUTED_JAR_TIMESTAMP = "distributedConnectorJarTimestamp";
    public static final String DISTRIBUTED_JAR_LEN = "distributedConnectorJarLen";

    public static final String DISTRIBUTED_CONNECTOR_CONF_LOCATION = "distributedConnectorConfLocation";
    public static final String DISTRIBUTED_CONNECTOR_CONF_TIMESTAMP = "distributedConnectorConfTimestamp";
    public static final String DISTRIBUTED_CONNECTOR_CONF_LEN = "distributedConnectorConfLen";

}

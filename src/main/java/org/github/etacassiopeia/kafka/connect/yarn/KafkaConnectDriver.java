package org.github.etacassiopeia.kafka.connect.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import static org.github.etacassiopeia.kafka.connect.yarn.Constants.*;

/**
 * <h1>TransporterDriver</h1>
 * The TransporterDriver
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 22/08/16
 */
public class KafkaConnectDriver {

    private static final Log LOG = LogFactory.getLog(KafkaConnectDriver.class);

    private YarnClient yarnClient;
    private Class applicationMasterClass;
    private YarnConfiguration conf;

    private final String appName = "Mule";

    public KafkaConnectDriver(Class appMasterMainClass, YarnConfiguration conf) {
        this.applicationMasterClass = appMasterMainClass;
        this.conf = conf;

        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage : java -cp <JarName.jar> <app_config_file> <worker_config_file>");
            System.exit(-1);
        }

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            System.exit(-1);
        }

        final KafkaConnectDriver kafkaConnectDriver = new KafkaConnectDriver(KafkaConnectAM.class, new YarnConfiguration());

        final ApplicationSubmissionContext applicationContext;
        try {
            applicationContext = kafkaConnectDriver.createApplicationContext(properties, args[1]);

            final ApplicationId appId = applicationContext.getApplicationId();

            Runtime.getRuntime().addShutdownHook(
                    new Thread() {
                        @Override
                        public void run() {
                            LOG.info("Shutting down the application " + appId);
                            try {
                                kafkaConnectDriver.forceKillApplication(appId);
                            } catch (IOException | YarnException e) {
                                LOG.error(e.getMessage(), e);
                            }
                        }
                    }
            );

            if (kafkaConnectDriver.submitApplication(applicationContext))
                LOG.info("Application completed successfully");
            else
                LOG.error("Application failed!");

        } catch (IOException | YarnException | URISyntaxException e) {
            LOG.error(e.getMessage(), e);
        }

    }

    public boolean submitApplication(ApplicationSubmissionContext applicationContext)
            throws IOException, YarnException {
        yarnClient.submitApplication(applicationContext);
        return monitorApplication(applicationContext.getApplicationId());
    }

    public void forceKillApplication(ApplicationId appId) throws YarnException, IOException {
        yarnClient.killApplication(appId);
    }

    public ApplicationSubmissionContext createApplicationContext(Properties properties, String connectorConfigFile)
            throws IOException, YarnException, URISyntaxException {
        yarnClient.start();

        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM"
                + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
                NodeState.RUNNING);
        LOG.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for"
                    + ", nodeId=" + node.getNodeId()
                    + ", nodeAddress" + node.getHttpAddress()
                    + ", nodeRackName" + node.getRackName()
                    + ", nodeNumContainers" + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo("default");
        LOG.info("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue"
                        + ", queueName=" + aclInfo.getQueueName()
                        + ", userAcl=" + userAcl.name());
            }
        }

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        int amMemory = Integer.parseInt(properties.getProperty(APP_MASTER_MEMORY, "1024"));

        // A resource ask cannot exceed the max.
        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem;
        }

        int amVCores = Integer.parseInt(properties.getProperty(APP_MASTER_VCORE, "1"));
        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();

        if (amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + amVCores
                    + ", max=" + maxVCores);
            amVCores = maxVCores;
        }

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

        appContext.setApplicationName(appName + "-" + properties.getProperty(JOB_ID));

        //Use this variable to submit configurations which are loaded from Config file
        //So we do not need to transfer the original config file
        Map<String, String> env = new HashMap<>();

        Map<String, LocalResource> localResources = new HashMap<>();
        FileSystem fs = FileSystem.get(conf);

        LocalResource jarRsrc = addToLocalResources(fs, getJarFromClass(applicationMasterClass),
                appContext.getApplicationId().toString(), localResources);

        env.put(DISTRIBUTED_JAR_LOCATION, getDistributedPathFromLocalResource(jarRsrc));
        env.put(DISTRIBUTED_JAR_LEN, Long.toString(jarRsrc.getSize()));
        env.put(DISTRIBUTED_JAR_TIMESTAMP, Long.toString(jarRsrc.getTimestamp()));


        LocalResource configRsrc = addToLocalResources(fs, new File(connectorConfigFile),
                appContext.getApplicationId().toString(), localResources);

        env.put(DISTRIBUTED_CONNECTOR_CONF_LOCATION, getDistributedPathFromLocalResource(configRsrc));
        env.put(DISTRIBUTED_CONNECTOR_CONF_LEN, Long.toString(configRsrc.getSize()));
        env.put(DISTRIBUTED_CONNECTOR_CONF_TIMESTAMP, Long.toString(configRsrc.getTimestamp()));

        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        env.put("CLASSPATH", classPathEnv.toString());

        Vector<CharSequence> vargs = new Vector<>(30);

        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-Xmx" + new Double(amMemory * 0.8).intValue() + "m");
        vargs.add(applicationMasterClass.getName());

        vargs.add(properties.getProperty(JOB_ID));
        vargs.add(properties.getProperty(CONTAINER_MEMORY));
        vargs.add(properties.getProperty(CONTAINER_VCORE));
        vargs.add(properties.getProperty(CONFIG_CONTAINER_COUNT));

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        List<String> commands = new ArrayList<>();
        commands.add(command.toString());

        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);

        appContext.setQueue("default");

        return appContext;

    }

    private File getJarFromClass(Class clazz) throws URISyntaxException {
        return new File(clazz.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
    }

    private String getDistributedPathFromLocalResource(LocalResource localResource) {
        URL resource = localResource.getResource();
        return resource.getScheme() + "://"
                + (resource.getScheme().startsWith("file") ? "" : resource.getHost()
                + (resource.getPort() != -1 ? (":" + resource.getPort()) : ""))
                + resource.getFile();
    }

    private LocalResource addToLocalResources(FileSystem fs, File fileSrc
            , String appId, Map<String, LocalResource> localResources) throws IOException {
        String suffix =
                appName + "/" + appId + "/" + fileSrc.getName();
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);

        fs.copyFromLocalFile(new Path(fileSrc.getCanonicalPath()), dst);

        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());

        localResources.put(fileSrc.getName(), scRsrc);

        return scRsrc;
    }

    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }
        }
    }
}


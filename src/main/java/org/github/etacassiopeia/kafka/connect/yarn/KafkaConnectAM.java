package org.github.etacassiopeia.kafka.connect.yarn;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.github.etacassiopeia.kafka.connect.yarn.Constants.*;

/**
 * <h1>TransporterAM</h1>
 * The TransporterAM
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 22/08/16
 */
public class KafkaConnectAM {
    private static final Log LOG = LogFactory.getLog(KafkaConnectAM.class);

    private YarnConfiguration conf;

    private AMRMClientAsync amRMClient;

    private NMCallbackHandler containerListener;

    private NMClientAsync nmClientAsync;

    private AtomicInteger numRequestedContainers = new AtomicInteger();

    private AtomicInteger numFailedContainers = new AtomicInteger();

    private List<Thread> launchThreads = new ArrayList<>();

    private volatile boolean done;

    private int containerMemory;

    private int containerVirtualCores;

    private int numContainers;

    private Map<String, String> envs;

    private ByteBuffer allTokens;

    private String jobId;

    public KafkaConnectAM(String jobId, int containerMemory, int containerVirtualCores, int numContainers) {
        this.containerMemory = containerMemory;
        this.containerVirtualCores = containerVirtualCores;
        this.jobId = jobId;
        this.numContainers = numContainers;
        conf = new YarnConfiguration();
    }

    public static void main(String[] args) {

        if (args.length < 4) {
            LOG.error("Wrong number of arguments. please supply <job-id> <container-memory> <container-vcore> <container-number>");
            return;
        }

        String jobId = args[0];
        int containerMemmory = Integer.parseInt(args[1]);
        int containerVCore = Integer.parseInt(args[2]);
        int containerNumber = Integer.parseInt(args[3]);

        final KafkaConnectAM kafkaConnectAM = new KafkaConnectAM(jobId, containerMemmory, containerVCore, containerNumber);

        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    kafkaConnectAM.finish();
                }
            });

            kafkaConnectAM.run();

            if (kafkaConnectAM.finish())
                LOG.info("Application Master completed successfully.");
            else
                LOG.info("Application Master failed.");
        } catch (InterruptedException | YarnException | IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public String getJobId() {
        return jobId;
    }

    //make a common method
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

    @SuppressWarnings({"unchecked"})
    public void run() throws YarnException, IOException, InterruptedException {

        configLog4j();

        envs = System.getenv();

        LOG.info("Starting AppMaster , " + "requiredContainers=" + numContainers);

        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);

        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(NetUtils.getHostname(),
                -1, "");

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info("There is/are " + previousAMRunningContainers.size()
                + " previous attempt's running container[s] on AM registration.");

        int numTotalContainersToRequest = numContainers - previousAMRunningContainers.size();

        LOG.info("Total number of required containers to request  : " + numTotalContainersToRequest);

        for (int i = 0; i < numTotalContainersToRequest; i++) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM(containerMemory, containerVirtualCores);
            amRMClient.addContainerRequest(containerAsk);
            numRequestedContainers.incrementAndGet();
            LOG.info("Container was requested!");
        }
    }

    public boolean finish() {
        while (!done) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }

        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
            }
        }

        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (numFailedContainers.get() == 0) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numContainers
                    + ", failed="
                    + numFailedContainers.get();
            LOG.info(appMessage);
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException | IOException ex) {
            LOG.error("Failed to unregister application", ex);
        }

        amRMClient.stop();

        return success;
    }

    private AMRMClient.ContainerRequest setupContainerAskForRM(int containerMemory, int containerVirtualCores) {
        Resource capability = Resource.newInstance(containerMemory,
                containerVirtualCores);

        //we can specify desired host which could be prefer to allocate resource from
        return new AMRMClient.ContainerRequest(capability, null, null,
                Priority.newInstance(0));
    }

    private NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            for (ContainerStatus containerStatus : completedContainers) {
                if (containerStatus.getExitStatus() != 0) {
                    // increment counters for failed containers
                    // transporter failed
                    numFailedContainers.incrementAndGet();

                    // container was killed by framework, possibly preempted
                    // we should re-try as the container was lost for some reason
                    numRequestedContainers.decrementAndGet();

                    LOG.info("Container failed ." + ", containerId="
                            + containerStatus.getContainerId() + " with exit code " + containerStatus.getExitStatus());

                    LOG.info("Requesting a container with " + containerMemory + " memory and " +
                            containerVirtualCores + " vcore");

                    AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM(containerMemory,
                            containerVirtualCores);


                    //Note : If the user has not removed container requests that have already been satisfied,
                    // then the re-register may end up sending the entire container requests
                    // to the RM (including matched requests). Which would mean the RM could end up giving it
                    // a lot of new allocated containers.
                    List<LinkedHashSet<AMRMClient.ContainerRequest>> matchingRequests = amRMClient.getMatchingRequests(
                            containerAsk.getPriority(), "*", containerAsk.getCapability());

                    for (LinkedHashSet<AMRMClient.ContainerRequest> col : matchingRequests) {
                        Object[] array = col.toArray();
                        for (Object o : array) {
                            AMRMClient.ContainerRequest req = (AMRMClient.ContainerRequest) o;
                            amRMClient.removeContainerRequest(req);
                        }
                    }

                    int askCount = numContainers - numRequestedContainers.get();

                    if (askCount > 0) {
                        for (int i = 0; i < askCount; i++) {
                            amRMClient.addContainerRequest(containerAsk);
                            numRequestedContainers.addAndGet(askCount);
                        }
                    }

                } else {
                    LOG.info("Container completed with exit code 0." + ", containerId="
                            + containerStatus.getContainerId());
                }
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Container allocated successfully." + ", containerId="
                        + allocatedContainer.getId());

                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);
                launchThreads.add(launchThread);
                launchThread.start();

                LOG.info("A new Task is added successfully." + ", containerId="
                        + allocatedContainer.getId());

            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> list) {

        }

        @Override
        public float getProgress() {
            return (float) numContainers / numRequestedContainers.get();
        }

        @Override
        public void onError(Throwable throwable) {
            done = true;
            amRMClient.stop();
        }
    }

    private static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private final KafkaConnectAM kafkaConnectAM;

        private ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<>();

        NMCallbackHandler(KafkaConnectAM kafkaConnectAM) {
            this.kafkaConnectAM = kafkaConnectAM;
        }

        void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
            Container container = containers.get(containerId);
            if (container != null) {
                kafkaConnectAM.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
                LOG.info("Container started successfully." + ", containerId="
                        + container.getId());
            }
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            containers.remove(containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable throwable) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            kafkaConnectAM.numFailedContainers.incrementAndGet();
            kafkaConnectAM.numRequestedContainers.decrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable throwable) {
            containers.remove(containerId);
        }
    }

    private class LaunchContainerRunnable implements Runnable {

        Container container;

        NMCallbackHandler containerListener;

        LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        @Override
        public void run() {
            Map<String, LocalResource> localResources = new HashMap<>();

            String jarPath = envs.get(Constants.DISTRIBUTED_JAR_LOCATION);
            URL jarYarnUrl;
            try {
                jarYarnUrl = ConverterUtils.getYarnUrlFromURI(new URI(jarPath));
            } catch (URISyntaxException e) {
                LOG.error("Error when trying to use jar file path specified"
                        + " in env, path=" + jarPath, e);
                numFailedContainers.incrementAndGet();
                numRequestedContainers.decrementAndGet();
                return;
            }

            int jarPathLen = Integer.parseInt(envs.get(Constants.DISTRIBUTED_JAR_LEN));
            long jarPathTimestamp = Long.parseLong(envs.get(Constants.DISTRIBUTED_JAR_TIMESTAMP));

            LocalResource jarRsrc = LocalResource.newInstance(jarYarnUrl,
                    LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                    jarPathLen, jarPathTimestamp);

            localResources.put(jarYarnUrl.getFile().substring(jarYarnUrl.getFile().lastIndexOf("/") + 1), jarRsrc);


            String connectorConfPath = envs.get(Constants.DISTRIBUTED_CONNECTOR_CONF_LOCATION);
            URL connectorConfYarnUrl;
            try {
                connectorConfYarnUrl = ConverterUtils.getYarnUrlFromURI(new URI(connectorConfPath));
            } catch (URISyntaxException e) {
                LOG.error("Error when trying to use jar file path specified"
                        + " in env, path=" + connectorConfPath, e);
                numFailedContainers.incrementAndGet();
                numRequestedContainers.decrementAndGet();
                return;
            }

            int connectorConfLen = Integer.parseInt(envs.get(Constants.DISTRIBUTED_CONNECTOR_CONF_LEN));
            long connectorConfTimestamp = Long.parseLong(envs.get(Constants.DISTRIBUTED_CONNECTOR_CONF_TIMESTAMP));

            LocalResource connectorConfRsrc = LocalResource.newInstance(connectorConfYarnUrl,
                    LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                    connectorConfLen, connectorConfTimestamp);

            localResources.put(connectorConfYarnUrl.getFile().substring(jarYarnUrl.getFile().lastIndexOf("/") + 1), connectorConfRsrc);

            Map<String, String> env = new HashMap<>();

            env.put(DISTRIBUTED_CONNECTOR_CONF_LOCATION, connectorConfPath);
            env.put(DISTRIBUTED_JAR_LEN, envs.get(Constants.DISTRIBUTED_CONNECTOR_CONF_LEN));
            env.put(DISTRIBUTED_JAR_TIMESTAMP, envs.get(Constants.DISTRIBUTED_CONNECTOR_CONF_TIMESTAMP));

            StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                    .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
            for (String c : conf.getStrings(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(c.trim());
            }

            // add the runtime classpath needed for tests to work
            if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
                classPathEnv.append(':');
                classPathEnv.append(System.getProperty("java.class.path"));
            }

            env.put("CLASSPATH", classPathEnv.toString());

            Vector<CharSequence> vargs = new Vector<>(30);

            vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
            /**
             * If you find that your system is spending too much time collecting garbage (your allocated virtual memory
             * is more than your RAM can handle), lower your heap size.
             * Typically, you should use 80 percent of the available RAM (not taken by the operating system or other
             * processes) for your JVM.

             * The heap sizes should be set to values such that the maximum amount of memory used by the VM
             * does not exceed the amount of available physical RAM. If this value is exceeded, the OS starts
             * paging and performance degrades significantly. The VM always uses more memory than the heap size.
             * The memory required for internal VM functionality, native libraries outside of the VM,
             * and  permanent generation memory (for the Sun VM only: memory required to store classes and methods)
             * is allocated in addition to the heap size settings
             */
            vargs.add("-Xmx" + new Double(containerMemory * 0.8).intValue() + "m");
            vargs.add(KafkaConnectWorker.class.getCanonicalName());

            vargs.add(connectorConfPath);

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Worker.stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Worker.stderr");

            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<>();
            commands.add(command.toString());

            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, env, commands, null, allTokens.duplicate(), null);
            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }
}


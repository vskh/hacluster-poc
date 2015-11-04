package poc.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import poc.cluster.config.ClusterConfig;
import poc.cluster.ha.FeatureAvailabilityListener;
import poc.cluster.ha.HighAvailabilityRegistry;
import poc.cluster.ha.hazelcast.HazelcastClusterMember;

public class ClusterApp {
    private static final Logger logger = LoggerFactory.getLogger(ClusterApp.class);

    public static void main(String[] args) {
        final ConfigurableApplicationContext ctx = new AnnotationConfigApplicationContext(ClusterConfig.class);

        final Runnable worker = new Runnable() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Worker is running");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.warn("Worker interrupted");
                        break;
                    }
                }
            }
        };

        final HighAvailabilityRegistry<HazelcastClusterMember, String> registry = (HighAvailabilityRegistry<HazelcastClusterMember, String>) ctx.getBean("haRegistry");
        registry.joinFeatureCluster("poc.cluster.ha", new FeatureAvailabilityListener<String, HazelcastClusterMember>() {

            private Thread workerThread = null;

            @Override
            public void onMasterRoleAssigned(String s) {
                if (workerThread != null) {
                    logger.error("Worker is not null when this node becomes master");
                } else {
                    workerThread = new Thread(worker);
                    workerThread.start();
                    logger.info("Started worker as this node becomes master");
                }
            }

            @Override
            public void onMasterChange(String s, HazelcastClusterMember self, HazelcastClusterMember oldMaster, HazelcastClusterMember newMaster) {
                logger.info("Feature '" + s + "' changes master from " + oldMaster + " to " + newMaster);
            }

            @Override
            public void onMasterRoleUnassigned(String s) {
                if (workerThread != null) {
                    workerThread.interrupt();
                    workerThread = null;
                    logger.info("Stopped worker as this node is no longer master");
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ctx.close();
                logger.info("Instance shut down");
            }
        });
    }
}

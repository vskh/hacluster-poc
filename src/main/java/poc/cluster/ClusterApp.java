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

        Runnable worker = new Runnable() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println(Thread.currentThread().getName() + " Worker is running");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.warn(Thread.currentThread().getName() + "Worker interrupted");
                        break;
                    }
                }
            }
        };

        final HighAvailabilityRegistry<HazelcastClusterMember, String> registry = (HighAvailabilityRegistry<HazelcastClusterMember, String>) ctx.getBean("haRegistry");
        registry.joinFeatureCluster("poc.cluster.ha0", new FeatureAvailabilityListenerImpl("poc.cluster.ha0", worker));

        if (registry.getLocalMember().hashCode() % 2 == 0) {
            registry.joinFeatureCluster("poc.cluster.ha1", new FeatureAvailabilityListenerImpl("poc.cluster.ha1", worker));
        } else {
            registry.joinFeatureCluster("poc.cluster.ha2", new FeatureAvailabilityListenerImpl("poc.cluster.ha2", worker));
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ctx.close();
                logger.info("Instance shut down");
            }
        });
    }
}

class FeatureAvailabilityListenerImpl implements FeatureAvailabilityListener<String, HazelcastClusterMember> {

    private final static Logger logger = LoggerFactory.getLogger(FeatureAvailabilityListener.class);

    private String feature;
    private Thread workerThread = null;
    private Runnable worker;

    public FeatureAvailabilityListenerImpl(String feature, Runnable worker) {
        this.feature = feature;
        this.worker = worker;
    }

    @Override
    public void onMasterRoleAssigned(String s) {
        if (workerThread != null) {
            logger.error(feature + ": Worker is not null when this node becomes master");
        } else {
            workerThread = new Thread(worker, feature);
            workerThread.start();
            logger.info(feature + ": Started worker as this node becomes master");
        }
    }

    @Override
    public void onMasterChange(String s, HazelcastClusterMember self, HazelcastClusterMember oldMaster, HazelcastClusterMember newMaster) {
        logger.info(feature + ": Feature changes master from " + oldMaster + " to " + newMaster);
    }

    @Override
    public void onMasterRoleUnassigned(String s) {
        if (workerThread != null) {
            workerThread.interrupt();
            workerThread = null;
            logger.info(feature + ": Stopped worker as this node is no longer master");
        }
    }
}

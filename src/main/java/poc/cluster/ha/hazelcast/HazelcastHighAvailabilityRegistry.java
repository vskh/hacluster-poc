package poc.cluster.ha.hazelcast;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryMergedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import poc.cluster.ha.FeatureAvailabilityListener;
import poc.cluster.ha.HighAvailabilityRegistry;
import poc.cluster.ha.MasterElector;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HazelcastHighAvailabilityRegistry is Hazelcast-based implementation of HighAvailabilityRegistry.
 *
 * @author vadya
 */
@Service("haRegistry")
public class HazelcastHighAvailabilityRegistry implements HighAvailabilityRegistry<HazelcastClusterMember, String> {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastHighAvailabilityRegistry.class);

    private static final String MEMBERSHIP_REGISTRY_ID = HazelcastHighAvailabilityRegistry.class.getCanonicalName() + ".subscriptions";
    private static final String FEATURE_MASTER_KEY = "FEATUREMASTER011235813";

    private HazelcastInstance hzInstance;
    private IMap<String, Map<String, HazelcastClusterMember>> membershipRegistry;
    private Map<String, FeatureAvailabilityListener<String, HazelcastClusterMember>> featureListeners =
            new ConcurrentHashMap<String, FeatureAvailabilityListener<String, HazelcastClusterMember>>();
    private MasterElector<HazelcastClusterMember> masterElector = new SmallestUuidMasterElector();

    private HazelcastClusterMember localMember = null;

    @Inject
    public HazelcastHighAvailabilityRegistry(HazelcastInstance hzInstance) {
        this.hzInstance = hzInstance;
        membershipRegistry = hzInstance.getMap(MEMBERSHIP_REGISTRY_ID);
        MapConfig mc;
        hzInstance.getCluster().addMembershipListener(new FailoverListener()); // tracking nodes going down
        membershipRegistry.addEntryListener(new FeatureMembershipListener(), true); // tracking subscriptions to features

        logger.info("[HA] Registry initialized on " + getLocalMember());
    }

    /**
     * Allows to override default master election algorithm.
     * Default algorithm uses node with smallest Uuid as master.
     *
     * @param masterElector custom MasterElector implementation
     */
    @Override
    public void setMasterElector(MasterElector<HazelcastClusterMember> masterElector) {
        this.masterElector = masterElector;
    }

    @Override
    public Collection<HazelcastClusterMember> getClusterMembers() {
        return convert(hzInstance.getCluster().getMembers());
    }

    @Override
    public synchronized HazelcastClusterMember getLocalMember() {
        if (localMember == null) {
            localMember = new HazelcastClusterMember(hzInstance.getCluster().getLocalMember());
        }

        return localMember;
    }

    @Override
    public Collection<HazelcastClusterMember> getMembersOf(String feature) {
        try {
            membershipRegistry.lock(feature);
            Map<String, HazelcastClusterMember> members = membershipRegistry.get(feature);

            return members == null ?
                    Collections.<HazelcastClusterMember>emptyList() :
                    Collections.unmodifiableCollection(members.values());
        } finally {
            membershipRegistry.unlock(feature);
        }
    }

    @Override
    public HazelcastClusterMember getClusterMaster() {
        return masterElector.elect(getClusterMembers());
    }

    @Override
    public HazelcastClusterMember getMasterMemberOf(String feature) {
        try {
            membershipRegistry.lock(feature);
            HazelcastClusterMember member = membershipRegistry.get(feature).get(FEATURE_MASTER_KEY);
            return member;
        } finally {
            membershipRegistry.unlock(feature);
        }
    }

    @Override
    public void joinFeatureCluster(String feature,
                                   FeatureAvailabilityListener<String, HazelcastClusterMember> listener) {
        try {
            membershipRegistry.lock(feature);

            if (featureListeners.containsKey(feature)) {
                throw new IllegalStateException("Already part of feature '" + feature + "' cluster");
            }

            ensureFeatureMembership(feature, listener);
        } finally {
            membershipRegistry.unlock(feature);
        }

        logger.info("[HA] " + localMember + " has joined '" + feature + "' cluster");
    }

    @Override
    public void leaveFeatureCluster(String feature) {
        try {
            membershipRegistry.lock(feature);

            FeatureAvailabilityListener featureAvailabilityListener = featureListeners.get(feature);
            if (featureAvailabilityListener == null) {
                throw new IllegalStateException("Not part of feature '" + feature + "' cluster");
            }

            Map<String, HazelcastClusterMember> featureMembers = membershipRegistry.get(feature);
            if (featureMembers == null) {
                logger.warn("[HA] Feature is missing from registry but present on local node");
            } else {
                HazelcastClusterMember localMember = getLocalMember();
                featureMembers.remove(localMember.getId());
                membershipRegistry.put(feature, featureMembers);
                logger.info("[HA] " + localMember + " has left '" + feature + "' cluster");
            }
            featureListeners.remove(feature);
        } finally {
            membershipRegistry.unlock(feature);
        }
    }

    private HazelcastClusterMember getClusterMaster(Collection<HazelcastClusterMember> members) {
        return masterElector.elect(members);
    }

    private Collection<HazelcastClusterMember> convert(Collection<Member> hzMembers) {
        ArrayList<HazelcastClusterMember> members = new ArrayList<HazelcastClusterMember>();
        for (Member m : hzMembers) {
            members.add(new HazelcastClusterMember(m));
        }

        return members;
    }

    private Map<String, HazelcastClusterMember> cleanupMembers(Map<String, HazelcastClusterMember> members) {
        Set<String> clusterMemberIds = new HashSet<String>();
        for (HazelcastClusterMember m : getClusterMembers()) {
            clusterMemberIds.add(m.getId());
        }
        Set<String> cleanedMemberIds = members.keySet();
        int preCleanSize = cleanedMemberIds.size();
        cleanedMemberIds.retainAll(clusterMemberIds);
        int postCleanSize = cleanedMemberIds.size();

        logger.debug("[HA] " + (preCleanSize - postCleanSize) + " members cleaned");

        return members;
    }

    /**
     * Handles case when cluster node goes down.
     *
     * @param goneMember     cluster member that went down.
     * @param clusterMembers new list of cluster members.
     */
    private void onMemberDown(Member goneMember, Collection<HazelcastClusterMember> clusterMembers) {
        HazelcastClusterMember member = new HazelcastClusterMember(goneMember);
        logger.debug("[HA] " + member + " is down");
        if (getClusterMaster(clusterMembers).equals(getLocalMember())) { // we are master, update cluster metadata
            for (String feature : membershipRegistry.keySet()) {
                try {
                    membershipRegistry.lock(feature);

                    Map<String, HazelcastClusterMember> members = membershipRegistry.get(feature);

                    if (members.containsKey(member.getId())) {
                        logger.trace("Removing " + member + " from '" + feature + "' cluster");
                        members.remove(member.getId());

                        membershipRegistry.put(feature, members);
                    }
                } finally {
                    membershipRegistry.unlock(feature);
                }
            }
        } else {
            logger.trace("[HA] Master = " + getClusterMaster() + ", local = " + getLocalMember());
        }
    }

    /**
     * Handles node announcements about membership in specific highly available features.
     *
     * @param feature feature identifier
     * @param members (member id -> member) map of nodes that provide high availability of given feature
     */
    private void onFeatureUpdate(String feature,
                                 HazelcastClusterMember oldMaster,
                                 HazelcastClusterMember newMaster,
                                 Map<String, HazelcastClusterMember> members) {
        logger.debug("[HA] '" + feature + "' cluster updated");
        if (getClusterMaster().equals(getLocalMember())) {
            try {
                membershipRegistry.lock(feature);
                newMaster = masterElector.elect(members.values());
                members.put(FEATURE_MASTER_KEY, newMaster);
                if (!newMaster.equals(oldMaster)) {
                    membershipRegistry.put(feature, members); // mind (almost) recursive call
                    logger.info("[HA-Master] Re-elected '" + feature + "' cluster master is " + newMaster.getId());
                }
            } finally {
                membershipRegistry.unlock(feature);
            }
        }

        if (!newMaster.equals(oldMaster)) {
            onFeatureMasterSwitch(feature, oldMaster, newMaster);
        }
    }

    /**
     * Handles fail over/fail back of master for particular feature.
     *
     * @param feature   feature id
     * @param oldMaster previous master (can be null if this is new feature in registry with single node yet)
     * @param newMaster new master
     */
    private void onFeatureMasterSwitch(String feature, HazelcastClusterMember oldMaster, HazelcastClusterMember newMaster) {
        HazelcastClusterMember self = getLocalMember();

        if (self.equals(oldMaster) || self.equals(newMaster)) {
            logger.info("[HA] '" + feature + "' cluster switch-over");

            FeatureAvailabilityListener<String, HazelcastClusterMember> featureListener =
                    featureListeners
                            .get(feature);

            if (self.equals(oldMaster)) {
                featureListener.onMasterRoleUnassigned(feature);
            } else if (self.equals(newMaster)) {
                featureListener.onMasterRoleAssigned(feature);
            }

            featureListener.onMasterChange(feature, self, oldMaster, newMaster);
        }
    }

    private void ensureFeatureMembership(String feature, FeatureAvailabilityListener<String, HazelcastClusterMember> listener) {
        try {
            membershipRegistry.lock(feature);
            HazelcastClusterMember localMember = getLocalMember();

            Map<String, HazelcastClusterMember> featureMembers = membershipRegistry.get(feature);
            if (featureMembers == null) {
                featureMembers = new TreeMap<String, HazelcastClusterMember>();
                featureMembers.put(FEATURE_MASTER_KEY, localMember); // initialize master to node that creates it
            }

            featureMembers.put(localMember.getId(), localMember);
            membershipRegistry.put(feature, featureMembers);
            featureListeners.put(feature, listener);
        } finally {
            membershipRegistry.unlock(feature);
        }
    }

    /**
     * Listens to Hazelcast cluster.
     */
    private class FailoverListener implements InitialMembershipListener {

        @Override
        public void init(InitialMembershipEvent event) {
            logger.trace("[HA] Init handler");
            logger.info("[HA] Registry failover listener initialized");
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            logger.trace("[HA] Member added handler");
            logger.debug("[HA] " + new HazelcastClusterMember(membershipEvent.getMember()) + " (re)joined");
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            logger.trace("[HA] Member removed handler");
            onMemberDown(membershipEvent.getMember(), convert(membershipEvent.getMembers()));
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) { /* not used */ }
    }

    /**
     * Listens to MEMBERSHIP_REGISTRY_ID map.
     */
    private class FeatureMembershipListener implements EntryAddedListener<String, Map<String, HazelcastClusterMember>>,
            EntryUpdatedListener<String, Map<String, HazelcastClusterMember>>,
            EntryMergedListener<String, Map<String, HazelcastClusterMember>> {

        @Override
        public void entryAdded(EntryEvent<String, Map<String, HazelcastClusterMember>> event) {
            logger.trace("[HA] Entry added handler");
            Map<String, HazelcastClusterMember> members = event.getValue();
            HazelcastClusterMember master = members.remove(FEATURE_MASTER_KEY);
            onFeatureUpdate(event.getKey(), null, master, members);
        }

        public void handleUpdate(String feature,
                                 Map<String, HazelcastClusterMember> oldMembers,
                                 Map<String, HazelcastClusterMember> newMembers) {
            HazelcastClusterMember oldMaster = oldMembers.get(FEATURE_MASTER_KEY);
            HazelcastClusterMember newMaster = newMembers.remove(FEATURE_MASTER_KEY);
            onFeatureUpdate(feature, oldMaster, newMaster, newMembers);
        }

        @Override
        public void entryUpdated(EntryEvent<String, Map<String, HazelcastClusterMember>> event) {
            logger.trace("[HA] Entry updated handler");
            if (event.getOldValue() != null) {
                // Hack: fire processing only if actual put/replace operation was done on key
                //       Can be used to avoid circular call to listener when source map is updated from within listener
                //       method.
                //       IMap.set() is used instead of IMap.put() to update value while not returning old value.
                handleUpdate(event.getKey(), event.getOldValue(), event.getValue());
            }
        }

        @Override
        public void entryMerged(EntryEvent<String, Map<String, HazelcastClusterMember>> event) {
            logger.trace("[HA] Entry merged handler");
            for (Map.Entry<String, FeatureAvailabilityListener<String, HazelcastClusterMember>> e : featureListeners.entrySet()) {
                ensureFeatureMembership(e.getKey(), e.getValue());
            }
            handleUpdate(event.getKey(), event.getMergingValue(), event.getValue());
        }
    }

    /**
     * Default master elector implementation.
     */
    private static class SmallestUuidMasterElector implements MasterElector<HazelcastClusterMember> {

        @Override
        public HazelcastClusterMember elect(Collection<HazelcastClusterMember> members) {
            SortedSet<HazelcastClusterMember> membersOrderedByUuid = new TreeSet<HazelcastClusterMember>(new Comparator<HazelcastClusterMember>() {
                @Override
                public int compare(HazelcastClusterMember o1, HazelcastClusterMember o2) {
                    return o1.getId().compareTo(o2.getId());
                }
            });
            membersOrderedByUuid.addAll(members);

            return membersOrderedByUuid.first();
        }
    }
}

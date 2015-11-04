package poc.cluster.ha.hazelcast;

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
    private static final String MASTERS_REGISTRY_ID = HazelcastHighAvailabilityRegistry.class.getCanonicalName() + ".masters";

    private HazelcastInstance hzInstance;
    private IMap<String, Map<String, HazelcastClusterMember>> membershipRegistry;
    private IMap<String, String> mastersRegistry;
    private Map<String, FeatureSubscription> featureListeners = new ConcurrentHashMap<>();
    private MasterElector<HazelcastClusterMember> masterElector = new SmallestUuidMasterElector();

    private HazelcastClusterMember localMember = null;

    @Inject
    public HazelcastHighAvailabilityRegistry(HazelcastInstance hzInstance) {
        this.hzInstance = hzInstance;
        membershipRegistry = hzInstance.getMap(MEMBERSHIP_REGISTRY_ID);
        mastersRegistry = hzInstance.getMap(MASTERS_REGISTRY_ID);

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
        membershipRegistry.lock(feature);
        Map<String, HazelcastClusterMember> members = membershipRegistry.get(feature);
        membershipRegistry.unlock(feature);

        return members == null ?
                Collections.<HazelcastClusterMember>emptyList() :
                Collections.unmodifiableCollection(members.values());
    }

    @Override
    public HazelcastClusterMember getClusterMaster() {
        return masterElector.elect(getClusterMembers());
    }

    @Override
    public HazelcastClusterMember getMasterMemberOf(String feature) {
        membershipRegistry.lock(feature);
        mastersRegistry.lock(feature);
        String masterId = mastersRegistry.get(feature);
        HazelcastClusterMember member = membershipRegistry.get(feature).get(masterId);
        mastersRegistry.unlock(feature);
        membershipRegistry.unlock(feature);

        return member;
    }

    @Override
    public void joinFeatureCluster(String feature,
                                   FeatureAvailabilityListener<String, HazelcastClusterMember> listener) {
        membershipRegistry.lock(feature);

        if (featureListeners.containsKey(feature)) {
            throw new IllegalStateException("Already part of feature '" + feature + "' cluster");
        }

        Map<String, HazelcastClusterMember> featureMembers = membershipRegistry.get(feature);
        if (featureMembers == null) {
            featureMembers = new TreeMap<String, HazelcastClusterMember>();
        }

        HazelcastClusterMember localMember = getLocalMember();
        featureMembers.put(localMember.getId(), localMember);

        membershipRegistry.put(feature, featureMembers);

        String masterChangeListenerId = mastersRegistry.addEntryListener(new MasterChangeListener(), feature, true);
        featureListeners.put(feature, new FeatureSubscription(listener, masterChangeListenerId));

        membershipRegistry.unlock(feature);
    }

    @Override
    public void leaveFeatureCluster(String feature) {
        membershipRegistry.lock(feature);

        FeatureSubscription featureSubscription = featureListeners.get(feature);
        if (featureSubscription == null) {
            throw new IllegalStateException("Not part of feature '" + feature + "' cluster");
        }

        Map<String, HazelcastClusterMember> featureMembers = membershipRegistry.get(feature);
        if (featureMembers == null) {
            logger.warn("[HA] Feature is missing from registry but present on local node");
        } else {
            featureMembers.remove(getLocalMember().getId());
            membershipRegistry.put(feature, featureMembers);
        }

        mastersRegistry.removeEntryListener(featureSubscription.masterChangeListenerId);
        featureListeners.remove(feature);

        membershipRegistry.unlock(feature);
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

    /**
     * Handles case when cluster node goes down.
     *
     * @param goneMember     cluster member that went down.
     * @param clusterMembers new list of cluster members.
     */
    private void onMemberDown(Member goneMember, Set<Member> clusterMembers) {
        HazelcastClusterMember member = new HazelcastClusterMember(goneMember);
        logger.debug("[HA] Node " + member.getId() + " is down");
        if (getClusterMaster(convert(clusterMembers)).equals(getLocalMember())) { // we are master, update cluster metadata
            for (String feature : membershipRegistry.keySet()) {
                membershipRegistry.lock(feature);

                Map<String, HazelcastClusterMember> members = membershipRegistry.get(feature);

                if (members.containsKey(member.getId())) {
                    members.remove(member.getId());
                    membershipRegistry.put(feature, members);

                    mastersRegistry.lock(feature);
                    if (member.getId().equals(mastersRegistry.get(feature))) {
                        String newMasterId = masterElector.elect(members.values()).getId();
                        mastersRegistry.put(feature, newMasterId);
                        logger.info("[HA-Master] Master of '" + feature + "' cluster is down. Re-elected master is " + newMasterId);
                    }
                    mastersRegistry.unlock(feature);
                }

                membershipRegistry.unlock(feature);
            }
        }
    }

    /**
     * Handles node announcements about membership in specific highly available features.
     *
     * @param feature feature identifier
     * @param members (member id -> member) map of nodes that provide high availability of given feature
     */
    private void onFeatureUpdate(String feature, Map<String, HazelcastClusterMember> members) {
        logger.debug("[HA] Feature '" + feature + "' membership updated");
        if (getClusterMaster().equals(getLocalMember())) {
            mastersRegistry.lock(feature);
            String newMasterId = masterElector.elect(members.values()).getId();
            if (!newMasterId.equals(mastersRegistry.get(feature))) {
                mastersRegistry.put(feature, newMasterId);
                logger.info("[HA-Master] New node has joined '" + feature + "' cluster. Re-elected master is " + newMasterId);
            }
            mastersRegistry.unlock(feature);
        }
    }

    /**
     * Handles fail over/fail back of master for particular feature.
     *
     * @param feature   feature id
     * @param oldMaster previous master uuid (can be null if this is new feature in registry with single node yet)
     * @param newMaster new master uuid
     */
    private void onFeatureMasterSwitch(String feature, String oldMaster, String newMaster) {
        membershipRegistry.lock(feature);
        Map<String, HazelcastClusterMember> members = membershipRegistry.get(feature);
        String selfId = getLocalMember().getId();

        if (selfId.equals(oldMaster) || selfId.equals(newMaster)) {
            logger.info("[HA] Feature '" + feature + "' switch-over");

            FeatureAvailabilityListener<String, HazelcastClusterMember> featureListener =
                    featureListeners
                            .get(feature)
                            .featureListener;

            if (selfId.equals(oldMaster)) {
                featureListener.onMasterRoleUnassigned(feature);
            } else if (selfId.equals(newMaster)) {
                featureListener.onMasterRoleAssigned(feature);
            }

            featureListener.onMasterChange(feature,
                    getLocalMember(),
                    oldMaster == null ? null : members.get(oldMaster),
                    newMaster == null ? null : members.get(newMaster));
        }
        membershipRegistry.unlock(feature);
    }

    /**
     * Listens to Hazelcast cluster.
     */
    private class FailoverListener implements InitialMembershipListener {

        @Override
        public void init(InitialMembershipEvent event) {
            logger.info("[HA] Registry failover listener initialized");
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) { /* not used */ }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            onMemberDown(membershipEvent.getMember(), membershipEvent.getMembers());
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
            onFeatureUpdate(event.getKey(), event.getValue());
        }

        @Override
        public void entryUpdated(EntryEvent<String, Map<String, HazelcastClusterMember>> event) {
            onFeatureUpdate(event.getKey(), event.getValue());
        }

        @Override
        public void entryMerged(EntryEvent<String, Map<String, HazelcastClusterMember>> event) {
            onFeatureUpdate(event.getKey(), event.getValue());
        }
    }

    /**
     * Listens to MASTERS_REGISTRY_ID map.
     */
    private class MasterChangeListener implements EntryAddedListener<String, String>,
            EntryUpdatedListener<String, String>,
            EntryMergedListener<String, String> {

        @Override
        public void entryAdded(EntryEvent<String, String> event) {
            onFeatureMasterSwitch(event.getKey(), event.getOldValue(), event.getValue());
        }

        @Override
        public void entryUpdated(EntryEvent<String, String> event) {
            onFeatureMasterSwitch(event.getKey(), event.getOldValue(), event.getValue());
        }

        @Override
        public void entryMerged(EntryEvent<String, String> event) {
            onFeatureMasterSwitch(event.getKey(), event.getOldValue(), event.getValue());
        }
    }

    /**
     * Handy holder.
     */
    private static class FeatureSubscription {
        public FeatureSubscription(FeatureAvailabilityListener<String, HazelcastClusterMember> featureListener,
                                   String masterChangeListenerId) {

            this.featureListener = featureListener;
            this.masterChangeListenerId = masterChangeListenerId;
        }

        FeatureAvailabilityListener<String, HazelcastClusterMember> featureListener;
        String masterChangeListenerId;
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

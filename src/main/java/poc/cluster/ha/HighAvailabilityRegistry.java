package poc.cluster.ha;

import java.util.Collection;

/**
 * Provides generic interface to registry of highly available features supported by cluster.
 *  Member defines type of cluster nodes.
 *  Feature defines type of descriptor of abstract functionality that can be registered for high
 * availability within cluster.
 *
 * Operation description:
 * Cluster contains number of nodes. Nodes within cluster can join groups that provide highly available service
 * with single master doing job at a time. Every time group membership changes HighAvailabilityRegistry elects single
 * master node (via pluggable algorithm) and notifies old master and new master via FeatureAvailabilityListener
 * callback about the change. Client code within callback can do necessary job to bring service up or down on
 * specific node.
 *
 * @author vadya
 */
public interface HighAvailabilityRegistry<Member, Feature> {

    /**
     * Sets custom cluster and feature master election algorithm.
     *
     * @param masterElector   custom MasterElector implementation
     */
    void setMasterElector(MasterElector<Member> masterElector);

    /**
     * Returns all currently available cluster members
     *
     * @return collection of members
     */
    Collection<Member> getClusterMembers();

    /**
     * Returns Member instance that corresponds to local node
     *
     * @return member for this node
     */
    Member getLocalMember();

    /**
     * Returns members of cluster that participate in high availability of given feature
     *
     * @param feature feature identifier
     * @return collection of members
     */
    Collection<Member> getMembersOf(Feature feature);

    /**
     * Returns current master of whole cluster (not particular highly available feature).
     * @return master member
     */
    Member getClusterMaster();

    /**
     * Returns current master for given feature
     * @param feature feature identifier
     * @return master member
     */
    Member getMasterMemberOf(Feature feature);

    /**
     * Registers this node to participate in high availability of given feature
     * @param feature feature identifier
     * @param listener callback to notify about feature availability
     */
    void joinFeatureCluster(Feature feature, FeatureAvailabilityListener<Feature, Member> listener);

    /**
     * Unregisters this node to participate in high availability of given feature
     * @param feature feature identifier
     */
    void leaveFeatureCluster(Feature feature);
}

package poc.cluster.ha;

/**
 * FeatureAvailabilityListener <Description>
 *
 * @author vadya
 */
public interface FeatureAvailabilityListener<Feature, Member> {
    /**
     * Happens when the node becomes master.
     *
     * NOTE: in split-brain recovery situation this method can be invoked when this node was previously master
     *       of partition.
     * @param feature   feature for which node becomes master
     */
    void onMasterRoleAssigned(Feature feature);

    /**
     * Happends when feature cluster master changes.
     * @param feature   feature being updated
     * @param self      this node
     * @param oldMaster previous master
     * @param newMaster new master
     */
    void onMasterChange(Feature feature, Member self, Member oldMaster, Member newMaster);
    /**
     * Happens when the node resigns as master.
     *
     * @param feature   related feature
     */
    void onMasterRoleUnassigned(Feature feature);
}

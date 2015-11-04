package poc.cluster.ha;

/**
 * FeatureAvailabilityListener <Description>
 *
 * @author vadya
 */
public interface FeatureAvailabilityListener<Feature, Member> {
    void onMasterRoleAssigned(Feature feature);
    void onMasterChange(Feature feature, Member self, Member oldMaster, Member newMaster);
    void onMasterRoleUnassigned(Feature feature);
}

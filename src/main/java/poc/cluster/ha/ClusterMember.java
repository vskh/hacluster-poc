package poc.cluster.ha;

/**
 * ClusterMember describes node participating in cluster.
 *
 * @author vadya
 */
public interface ClusterMember<IDType> {
    IDType getId();
    String getAttribute(String attrName);
    void setAttribute(String attrName, String attrValue);
}

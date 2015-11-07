package poc.cluster.ha;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * ClusterMember describes node participating in cluster.
 *
 * @author vadya
 */
public interface ClusterMember<IDType> {
    IDType getId();
    String getAttribute(String attrName);
    void setAttribute(String attrName, String attrValue) throws NotImplementedException;
}

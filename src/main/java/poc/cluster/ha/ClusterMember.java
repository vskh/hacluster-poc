package poc.cluster.ha;

/**
 * ClusterMember describes node participating in cluster.
 *
 * @author vadya
 */
public interface ClusterMember<IDType> {
    IDType getId();
}

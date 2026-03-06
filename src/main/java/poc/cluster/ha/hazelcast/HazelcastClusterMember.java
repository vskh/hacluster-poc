package poc.cluster.ha.hazelcast;

import com.hazelcast.cluster.Member;
import poc.cluster.ha.ClusterMember;

import java.io.Serializable;

/**
 * Hazelcast implementation of ClusterMember
 *
 * @author vadya
 */
public class HazelcastClusterMember implements ClusterMember<String>, Serializable {

    private String uuid;
    private transient Member nativeMember;

    public HazelcastClusterMember(Member member) {
        this.uuid = member.getAddress().toString(); // addresses are used instead of uuids as they might change on
                                                          // clusters merge after split-brain.
        this.nativeMember = member;
    }

    @Override
    public String getId() {
        return uuid;
    }

    @Override
    public String getAttribute(String attrName) {
        return (String) nativeMember.getAttribute(attrName);
    }

    @Override
    public void setAttribute(String attrName, String attrValue) {
        // In Hazelcast 5.x, member attributes are immutable at runtime
        // and can only be set via configuration before starting the member.
        throw new UnsupportedOperationException("Setting member attributes at runtime is not supported in Hazelcast 5.x. Use member configuration instead.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HazelcastClusterMember that = (HazelcastClusterMember) o;

        return uuid.equals(that.uuid);

    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public String toString() {
        return "HazelcastClusterMember{" +
                "uuid='" + uuid + '\'' +
                '}';
    }
}

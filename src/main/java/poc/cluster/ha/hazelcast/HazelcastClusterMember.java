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

    private String memberId;
    private transient Member nativeMember;

    public HazelcastClusterMember(Member member) {
        // Build a stable identifier from explicit address components instead of Address.toString().
        // Addresses are used instead of UUIDs as they might change on clusters merge after split-brain.
        String host = member.getAddress().getHost();
        int port = member.getAddress().getPort();
        this.memberId = host + ":" + port;
        this.nativeMember = member;
    }

    @Override
    public String getId() {
        return memberId;
    }

    @Override
    public String getAttribute(String attrName) {
        Object value = nativeMember.getAttribute(attrName);
        return value != null ? value.toString() : null;
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

        return memberId.equals(that.memberId);

    }

    @Override
    public int hashCode() {
        return memberId.hashCode();
    }

    @Override
    public String toString() {
        return "HazelcastClusterMember{" +
                "memberId='" + memberId + '\'' +
                '}';
    }
}

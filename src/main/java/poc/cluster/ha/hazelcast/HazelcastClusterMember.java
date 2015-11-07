package poc.cluster.ha.hazelcast;

import com.hazelcast.core.Member;
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
        this.uuid = member.getSocketAddress().toString(); // addresses are used instead of uuids as they might change on
                                                          // clusters merge after split-brain.
        this.nativeMember = member;
    }

    @Override
    public String getId() {
        return uuid;
    }

    @Override
    public String getAttribute(String attrName) {
        return nativeMember.getStringAttribute(attrName);
    }

    @Override
    public void setAttribute(String attrName, String attrValue) {
        if (nativeMember == null) {
            throw new IllegalStateException("Setting attributes of non-local cluster members is not supported");
        }
        nativeMember.setStringAttribute(attrName, attrValue);
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

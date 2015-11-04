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

    public HazelcastClusterMember(Member member) {
        this.uuid = member.getUuid();
    }

    @Override
    public String getId() {
        return uuid;
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

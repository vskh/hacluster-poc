package poc.cluster.ha;

import java.util.Collection;

/**
 * MasterElector defines interface of pluggable master election algorithm.
 *
 * @author vadya
 */
public interface MasterElector<Member> {
    Member elect(Collection<Member> members);
}

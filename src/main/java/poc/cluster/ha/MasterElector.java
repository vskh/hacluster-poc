package poc.cluster.ha;

import java.util.Collection;

/**
 * MasterElector defines interface of pluggable master election algorithm.
 *
 * NOTE: to avoid split-brain cases election mechanism must not have side effects:
 * on any node provided same members input list it must elect same master.
 *
 * @author vadya
 */
public interface MasterElector<Member> {
    Member elect(Collection<Member> members);
}

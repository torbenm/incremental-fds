package org.mp.naumann.algorithms.fd.incremental.agreesets;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.Factory;

public class AgreeSetCollection {

    private final Map<OpenBitSet, ViolationSet> violations = new HashMap<>();
    private final Multimap<Integer, OpenBitSet> index = HashMultimap.create();
    private final Factory<ViolationSet> factory;

    public AgreeSetCollection(Factory<ViolationSet> factory) {
        this.factory = factory;
    }

    public void addAgreeSet(OpenBitSet agreeSet, int rec1, int rec2) {
        ViolationSet set = violations.computeIfAbsent(agreeSet, k -> factory.create());
        set.add(rec1, rec2);
        index.put(rec1, agreeSet);
        index.put(rec2, agreeSet);
    }

    public Set<OpenBitSet> analyzeDiff(CompressedDiff diff) {
        Set<OpenBitSet> agreeSets = new HashSet<>(violations.size());
        Set<Integer> deleted = diff.getDeletedRecords().keySet();
        for (int delete : deleted) {
            Collection<OpenBitSet> removed = index.removeAll(delete);
            agreeSets.addAll(removed);
        }
        for (OpenBitSet agreeSet : agreeSets) {
            ViolationSet set = violations.get(agreeSet);
            if (set != null) {
                Iterator<ViolatingPair> it = set.iterator();
                while (it.hasNext()) {
                    ViolatingPair pair = it.next();
                    if (pair.intersects(deleted)) {
                        it.remove();
                    }
                }
                if (set.isEmpty()) {
                    violations.remove(agreeSet);
                }
            }
        }
        return violations.keySet();
    }

    public interface ViolationSet extends Iterable<ViolatingPair> {

        void add(int a, int b);

        boolean isEmpty();
    }

}

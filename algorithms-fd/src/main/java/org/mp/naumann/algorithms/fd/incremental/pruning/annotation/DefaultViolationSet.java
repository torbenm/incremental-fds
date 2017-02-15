package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.DeletePruner.ViolationSet;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DefaultViolationSet implements ViolationSet {

    protected Set<ViolatingPair> set = new HashSet<>();

    @Override
    public void add(ViolatingPair violatingPair) {
        set.add(violatingPair);
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public Iterator<ViolatingPair> iterator() {
        return set.iterator();
    }
}

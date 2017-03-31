package org.mp.naumann.algorithms.fd.incremental.agreesets;

import org.mp.naumann.algorithms.fd.incremental.agreesets.AgreeSetCollection.ViolationSet;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DefaultViolationSet implements ViolationSet {

    protected Set<ViolatingPair> set = new HashSet<>();

    @Override
    public void add(int a, int b) {
        set.add(new ViolatingPair(a, b));
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

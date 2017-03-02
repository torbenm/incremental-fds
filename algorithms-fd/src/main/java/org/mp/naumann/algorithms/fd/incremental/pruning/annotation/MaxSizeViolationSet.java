package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

public class MaxSizeViolationSet extends DefaultViolationSet {

    private final int maxSize;

    public MaxSizeViolationSet(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public void add(ViolatingPair violatingPair) {
        if (set.size() >= maxSize) {
            return;
        }
        super.add(violatingPair);
    }
}

package org.mp.naumann.algorithms.fd.incremental.agreesets;

public class MaxSizeViolationSet extends DefaultViolationSet {

    private final int maxSize;

    public MaxSizeViolationSet(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public void add(int a, int b) {
        if (set.size() >= maxSize) {
            return;
        }
        super.add(a, b);
    }
}

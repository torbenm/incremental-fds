package org.mp.naumann.algorithms.fd.incremental.test;

import org.apache.lucene.util.OpenBitSet;

public class LhsRhsPair {

    private final OpenBitSet lhs;
    private final LatticeElement element;

    public LhsRhsPair(OpenBitSet lhs, LatticeElement element) {
        this.lhs = lhs;
        this.element = element;
    }

    public OpenBitSet getLhs() {
        return lhs;
    }

    public LatticeElement getElement() {
        return element;
    }
}

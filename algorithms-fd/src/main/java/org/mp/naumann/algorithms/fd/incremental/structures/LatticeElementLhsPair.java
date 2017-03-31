package org.mp.naumann.algorithms.fd.incremental.structures;

import org.apache.lucene.util.OpenBitSet;

public class LatticeElementLhsPair {

    private final OpenBitSet lhs;
    private final LatticeElement element;

    public LatticeElementLhsPair(OpenBitSet lhs, LatticeElement element) {
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

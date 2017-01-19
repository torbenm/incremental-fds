package org.mp.naumann.algorithms.fd.structures;

import org.apache.lucene.util.OpenBitSet;

public class OpenBitSetFD {
    private OpenBitSet lhs;
    private int rhs;
    public OpenBitSetFD(OpenBitSet lhs, int rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public OpenBitSet getLhs() {
        return lhs;
    }

    public int getRhs() {
        return rhs;
    }
}

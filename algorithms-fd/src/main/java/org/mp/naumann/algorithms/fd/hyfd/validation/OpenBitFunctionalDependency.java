package org.mp.naumann.algorithms.fd.hyfd.validation;

import org.apache.lucene.util.OpenBitSet;

class OpenBitFunctionalDependency {
    private OpenBitSet lhs;
    private int rhs;

    OpenBitFunctionalDependency(OpenBitSet lhs, int rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public OpenBitSet getLhs() {
        return lhs;
    }

    public void setLhs(OpenBitSet lhs) {
        this.lhs = lhs;
    }

    public int getRhs() {
        return rhs;
    }

    public void setRhs(int rhs) {
        this.rhs = rhs;
    }
}


package org.mp.naumann.algorithms.fd.structures;

import org.apache.lucene.util.OpenBitSet;

import java.util.List;
import java.util.Set;

public class FDViolation {

    private final OpenBitSet lhs;
    private final OpenBitSet rhs;
    private final Set<List<Object>> violatingValues;


    public FDViolation(OpenBitSet lhs, OpenBitSet rhs, Set<List<Object>> violatingValues) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.violatingValues = violatingValues;
    }

    public OpenBitSet getLhs() {
        return lhs;
    }

    public OpenBitSet getRhs() {
        return rhs;
    }

    public Set<List<Object>> getViolatingValues() {
        return violatingValues;
    }
}

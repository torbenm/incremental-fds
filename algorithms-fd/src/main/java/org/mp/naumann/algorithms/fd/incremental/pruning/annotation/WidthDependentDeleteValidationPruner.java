package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.pruning.CardinalitySet;

public class WidthDependentDeleteValidationPruner extends SimpleDeleteValidationPruner {
    public WidthDependentDeleteValidationPruner(CardinalitySet violations) {
        super(violations);
    }

    @Override
    public boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet originalRhs) {
        if(lhs.cardinality() > 2) {
            return super.doesNotNeedValidation(lhs, originalRhs);
        }
        return false;
    }
}

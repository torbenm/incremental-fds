package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;

public class WidthDependentValidationPrunerDecorator implements ValidationPruner {
    private final ValidationPruner pruner;

    public WidthDependentValidationPrunerDecorator(ValidationPruner pruner) {
        this.pruner = pruner;
    }

    @Override
    public boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet originalRhs) {
        return lhs.cardinality() > 2 && pruner.doesNotNeedValidation(lhs, originalRhs);
    }
}

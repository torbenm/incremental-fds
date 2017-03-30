package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

import java.util.Set;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;

public class SimpleDeleteValidationPruner implements ValidationPruner {

    private final Set<OpenBitSet> agreeSets;

    public SimpleDeleteValidationPruner(Set<OpenBitSet> agreeSets) {
        this.agreeSets = agreeSets;
    }

    @Override
    public boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet originalRhs) {
        return agreeSets.contains(lhs);
    }
}

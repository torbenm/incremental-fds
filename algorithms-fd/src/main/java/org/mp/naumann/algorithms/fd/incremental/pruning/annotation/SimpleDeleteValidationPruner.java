package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.pruning.CardinalitySet;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

public class SimpleDeleteValidationPruner implements ValidationPruner {

    private final CardinalitySet violations;

    public SimpleDeleteValidationPruner(CardinalitySet violations) {
        this.violations = violations;
    }

    @Override
    public boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet originalRhs) {
        OpenBitSet rhs = originalRhs.clone();
        for (int level = violations.getDepth(); level >= lhs.cardinality(); level--) {
            ObjectOpenHashSet<OpenBitSet> violationLevel = violations.getLevel(level);
            for (OpenBitSet violation : violationLevel) {
                if (BitSetUtils.isContained(lhs, violation)) {
                    // records agree in lhs
                    // remove bits from rhs where records disagree
                    rhs.and(violation);
                    if (rhs.isEmpty()) {
                        // there was disagreement for every rhs bit -> fd still invalid
                        return true;
                    }
                }
            }
        }
        return false;
    }
}

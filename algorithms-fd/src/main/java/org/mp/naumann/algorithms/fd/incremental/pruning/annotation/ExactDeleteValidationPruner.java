package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Set;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.pruning.CardinalitySet;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

public class ExactDeleteValidationPruner implements ValidationPruner {

    private final CardinalitySet violations;

    public ExactDeleteValidationPruner(Set<OpenBitSet> agreeSets, int numAttributes) {
        violations = new CardinalitySet(numAttributes);
        for (OpenBitSet agreeSet : agreeSets) {
            violations.add(agreeSet);
        }
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

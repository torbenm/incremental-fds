package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.hyfd.FDList;
import org.mp.naumann.algorithms.fd.incremental.structures.Lattice;

import java.util.List;

class FDInductor {

    private final IncrementalInductor inductor;
    private final int numAttributes;

    FDInductor(Lattice posCover, Lattice negCover, int numAttributes) {
        this.numAttributes = numAttributes;
        this.inductor = new IncrementalInductor(posCover, negCover, numAttributes);
    }

    int updatePositiveCover(FDList agreeSets) {
        int newFds = 0;
        // Level means number of set bits, e.g. 01010101 -> Level 4.
        // We start with higher levels because theyre more general
        for (int i = agreeSets.getFdLevels().size() - 1; i >= 0; i--) {
            if (i >= agreeSets.getFdLevels().size()) { // If this level has been trimmed during iteration
                continue;
            }

            List<OpenBitSet> agreeSetLevel = agreeSets.getFdLevels().get(i);
            for (OpenBitSet lhs : agreeSetLevel) {

                // All 0s cannot be on the right-hand side anymore if 01010101 is the LHS.
                // Thus flipping gives us the full rhs
                OpenBitSet fullRhs = lhs.clone();
                fullRhs.flip(0, numAttributes);

                // Now we go through all "not-rhs's" and specialize the pos. cover with them.
                for (int rhs = fullRhs.nextSetBit(0); rhs >= 0; rhs = fullRhs.nextSetBit(rhs + 1)) {
                    newFds += inductor.deduceDependencies(lhs, rhs);
                }
            }
            agreeSetLevel.clear();
        }
        return newFds;
    }

}

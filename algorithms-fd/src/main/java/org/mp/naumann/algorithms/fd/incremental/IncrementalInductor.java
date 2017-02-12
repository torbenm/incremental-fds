package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.hyfd.FDList;
import org.mp.naumann.algorithms.fd.structures.Lattice;

import java.util.List;

class IncrementalInductor {

    private Lattice negCover;
    private Lattice posCover;
    private final int numAttributes;

    IncrementalInductor(Lattice negCover, Lattice posCover, int numAttributes) {
        this.negCover = negCover;
        this.posCover = posCover;
        this.numAttributes = numAttributes;
    }

    void updatePositiveCover(FDList nonFds) {
        // Level means number of set bits, e.g. 01010101 -> Level 4.
        // We start with higher levels because theyre more general
        for (int i = nonFds.getFdLevels().size() - 1; i >= 0; i--) {
            if (i >= nonFds.getFdLevels().size()) { // If this level has been trimmed during iteration
                continue;
            }

            List<OpenBitSet> nonFdLevel = nonFds.getFdLevels().get(i);
            for (OpenBitSet lhs : nonFdLevel) {

                // All 0s cannot be on the right-hand side anymore if 01010101 is the LHS.
                // Thus flipping gives us the full rhs
                OpenBitSet fullRhs = lhs.clone();
                fullRhs.flip(0, fullRhs.size());

                // Now we go through all "not-rhs's" and specialize the pos. cover with them.
                for (int rhs = fullRhs.nextSetBit(0); rhs >= 0; rhs = fullRhs.nextSetBit(rhs + 1)) {
                    this.specializePositiveCover(lhs, rhs);
                }
            }
            nonFdLevel.clear();
        }
    }

    private void specializePositiveCover(OpenBitSet lhs, int rhs) {
        int numAttributes = this.posCover.getChildren().length;
        int newFDs = 0;
        List<OpenBitSet> specLhss;
        if (!(specLhss = this.posCover.getFdAndGeneralizations(lhs, rhs)).isEmpty()) { // TODO: May be "while" instead of "if"?
            for (OpenBitSet specLhs : specLhss) {
                this.posCover.removeFunctionalDependency(specLhs, rhs);

                OpenBitSet flipped = flip(specLhs);
                if (!negCover.containsFdOrGeneralization(flipped, rhs)) {
                    negCover.addFunctionalDependency(flipped, rhs);
                    negCover.removeSpecializations(flipped, rhs);
                }

                for (int attr = numAttributes - 1; attr >= 0; attr--) { // TODO: Is iterating backwards a good or bad idea?
                    if (!lhs.get(attr) && (attr != rhs)) {
                        specLhs.fastSet(attr);

                        if (!this.posCover.containsFdOrGeneralization(specLhs, rhs)) {
                            this.posCover.addFunctionalDependency(specLhs, rhs);
                            newFDs++;
                        }
                        specLhs.fastClear(attr);
                    }
                }
            }
        }
    }

    private OpenBitSet flip(OpenBitSet lhs) {
        OpenBitSet flipped = lhs.clone();
        flipped.flip(0, numAttributes);
        return flipped;
    }

}

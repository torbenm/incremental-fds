package org.mp.naumann.algorithms.fd.incremental.test;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.hyfd.FDList;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.List;
import java.util.logging.Level;

class Inductor {

    private Lattice negCover;
    private Lattice posCover;
    private final int numAttributes;

    public Inductor(Lattice negCover, Lattice posCover, int numAttributes) {
        this.negCover = negCover;
        this.posCover = posCover;
        this.numAttributes = numAttributes;
    }

    public void updatePositiveCover(FDList nonFds) {
        FDLogger.log(Level.FINER, "Inducing FD candidates ...");
        // Level means number of set bits, e.g. 01010101 -> Level 4.
        // We start with higher levels because theyre more general
        for (int i = nonFds.getFdLevels().size() - 1; i >= 0; i--) {
            if (i >= nonFds.getFdLevels().size()) // If this level has been trimmed during iteration
                continue;

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

    private int specializePositiveCover(OpenBitSet lhs, int rhs) {
        int numAttributes = this.posCover.getChildren().length;
        int newFDs = 0;
        List<OpenBitSet> specLhss;
        if (!(specLhss = this.posCover.getFdAndGeneralizations(new OpenBitSetFD(lhs, rhs))).isEmpty()) { // TODO: May be "while" instead of "if"?
            for (OpenBitSet specLhs : specLhss) {
                OpenBitSetFD specFd = new OpenBitSetFD(specLhs, rhs);
                this.posCover.removeFunctionalDependency(specFd);

                OpenBitSetFD flipped = flip(specFd);
                if (!negCover.containsFdOrGeneralization(flipped)) {
                    negCover.addFunctionalDependency(flipped);
                    negCover.removeSpecializations(flipped);
                }

                for (int attr = numAttributes - 1; attr >= 0; attr--) { // TODO: Is iterating backwards a good or bad idea?
                    if (!lhs.get(attr) && (attr != rhs)) {
                        specLhs.set(attr);

                        if (!this.posCover.containsFdOrGeneralization(specFd)) {
                            this.posCover.addFunctionalDependency(specFd);
                            newFDs++;
                        }
                        specLhs.clear(attr);
                    }
                }
            }
        }

        return newFDs;
    }

    private OpenBitSetFD flip(OpenBitSetFD fd) {
        OpenBitSet lhs = fd.getLhs().clone();
        lhs.flip(0, numAttributes);
        return new OpenBitSetFD(lhs, fd.getRhs());
    }

}

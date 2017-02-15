package org.mp.naumann.algorithms.fd.structures;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

public class LatticeBuilder {

    private final Lattice fds;
    private final Lattice nonFds;
    private final int numAttributes;

    private LatticeBuilder(int numAttributes) {
        fds = new Lattice(numAttributes);
        nonFds = new Lattice(numAttributes);
        this.numAttributes = numAttributes;
    }

    public static LatticeBuilder build(FDTree posCover) {
        int numAttributes = posCover.getNumAttributes();
        List<OpenBitSetFD> functionalDependencies = posCover.getFunctionalDependencies();
        return build(numAttributes, functionalDependencies);
    }

    private static LatticeBuilder build(int numAttributes, List<OpenBitSetFD> functionalDependencies) {
        LatticeBuilder builder = new LatticeBuilder(numAttributes);
        builder.buildPositiveCover(functionalDependencies);
        builder.buildNegativeCover();
        return builder;
    }

    private void buildPositiveCover(List<OpenBitSetFD> fds) {
        for (OpenBitSetFD fd : fds) {
            this.fds.addFunctionalDependency(fd.getLhs(), fd.getRhs());
        }
    }

    public Lattice getFds() {
        return fds;
    }

    public Lattice getNonFds() {
        return nonFds;
    }

    private void buildNegativeCover() {
        FDLogger.log(Level.FINER, "Building negative cover");
        List<LhsRhsPair> currentLevel = Collections.singletonList(new LhsRhsPair(new OpenBitSet(numAttributes), new OpenBitSet(numAttributes)));
        while (!currentLevel.isEmpty()) {
            for (LhsRhsPair current : currentLevel) {
                for (int rhs = current.getRhs().nextSetBit(0); rhs >= 0; rhs = current.getRhs().nextSetBit(rhs + 1)) {
                    OpenBitSet flipped = current.getLhs().clone();
                    flipped.flip(0, numAttributes);
                    if (!isValidFd(flipped, rhs)) {
                        current.getRhs().fastClear(rhs);
                        if (isMaximal(current.getLhs(), rhs)) {
                            nonFds.addFunctionalDependency(current.getLhs(), rhs);
                        }
                    }
                }
            }
            List<LhsRhsPair> nextLevel = new ArrayList<>();
            for (LhsRhsPair current : currentLevel) {
                int nextSetBit = current.getLhs().nextSetBit(0);
                if (nextSetBit < 0) {
                    nextSetBit = numAttributes;
                }
                for (int lhsAttr = 0; lhsAttr < nextSetBit; lhsAttr++) {
                    OpenBitSet lhs = current.getLhs().clone();
                    lhs.fastSet(lhsAttr);
                    OpenBitSet rhs = current.getRhs().clone();
                    rhs.fastSet(lhsAttr);
                    nextLevel.add(new LhsRhsPair(lhs, rhs));
                }
            }
            currentLevel = nextLevel;
        }
        FDLogger.log(Level.FINER, "Finsihed building negative cover");
    }

    private boolean isMaximal(OpenBitSet lhs, int rhs) {
        return !nonFds.containsFdOrGeneralization(lhs, rhs);
    }

    private boolean isValidFd(OpenBitSet lhs, int rhs) {
        return fds.containsFdOrGeneralization(lhs, rhs);
    }

    private static class LhsRhsPair {
        private final OpenBitSet lhs;
        private final OpenBitSet rhs;

        private LhsRhsPair(OpenBitSet lhs, OpenBitSet rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        public OpenBitSet getLhs() {
            return lhs;
        }

        public OpenBitSet getRhs() {
            return rhs;
        }
    }
}

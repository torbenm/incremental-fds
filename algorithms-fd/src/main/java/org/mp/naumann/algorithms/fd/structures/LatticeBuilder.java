package org.mp.naumann.algorithms.fd.structures;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.logging.Level;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;

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

    public static LatticeBuilder build(int numAttributes,
        List<OpenBitSetFD> functionalDependencies) {
        LatticeBuilder builder = new LatticeBuilder(numAttributes);
        builder.buildPositiveCover(functionalDependencies);
        builder.buildNegativeCover(functionalDependencies);
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

    private void buildNegativeCover(List<OpenBitSetFD> validFds) {
        FDLogger.log(Level.FINER, "Building negative cover");
        Multimap<Integer, OpenBitSet> groupedLhs = HashMultimap.create();
        for (OpenBitSetFD fd : validFds) {
            groupedLhs.put(fd.getRhs(), fd.getLhs());
        }
        for (int rhs = 0; rhs < numAttributes; rhs++) {
            OpenBitSet initial = new OpenBitSet(numAttributes);
            initial.fastSet(rhs);
            nonFds.addFunctionalDependency(initial, rhs);
            for (OpenBitSet lhs : groupedLhs.get(rhs)) {
                OpenBitSet flipped = lhs.clone();
                flipped.flip(0, numAttributes);
                List<OpenBitSet> invalidFds = nonFds.getFdAndGeneralizations(flipped, rhs);
                for (OpenBitSet invalidFd : invalidFds) {
                    nonFds.removeFunctionalDependency(invalidFd, rhs);
                    for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
                        if (invalidFd.fastGet(lhsAttr)) {
                            continue;
                        }
                        OpenBitSet generalizedLhs = invalidFd.clone();
                        generalizedLhs.fastSet(lhsAttr);
                        if (!nonFds.containsFdOrGeneralization(generalizedLhs, rhs)) {
                            nonFds.addFunctionalDependency(generalizedLhs, rhs);
                        }
                    }
                }
            }
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

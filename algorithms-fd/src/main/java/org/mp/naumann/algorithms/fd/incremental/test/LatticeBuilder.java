package org.mp.naumann.algorithms.fd.incremental.test;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LatticeBuilder {

    private final Lattice validFds;
    private final Lattice invalidFDs;
    private final int numAttributes;

    LatticeBuilder(int numAttributes) {
        validFds = new Lattice(numAttributes);
        invalidFDs = new Lattice(numAttributes);
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
            validFds.addFunctionalDependency(fd);
        }
    }

    public Lattice getValidFds() {
        return validFds;
    }

    public Lattice getInvalidFds() {
        return invalidFDs;
    }

    private void buildNegativeCover() {
        List<OpenBitSet> currentLevel = Collections.singletonList(new OpenBitSet(numAttributes));
        while (!currentLevel.isEmpty()) {
            for (OpenBitSet currentLhs : currentLevel) {
                for (int rhs = currentLhs.nextSetBit(0); rhs >= 0; rhs = currentLhs.nextSetBit(rhs + 1)) {
                    OpenBitSet flipped = currentLhs.clone();
                    flipped.flip(0, numAttributes);
                    OpenBitSetFD fd = new OpenBitSetFD(flipped, rhs);
                    if (!isValidFd(fd)) {
                        OpenBitSetFD nonFd = new OpenBitSetFD(currentLhs, rhs);
                        if (isMaximal(nonFd)) {
                            invalidFDs.addFunctionalDependency(nonFd);
                        }
                    }
                }
            }
            List<OpenBitSet> nextLevel = new ArrayList<>();
            for (OpenBitSet currentLhs : currentLevel) {
                int nextSetBit = currentLhs.nextSetBit(0);
                if (nextSetBit < 0) {
                    nextSetBit = numAttributes;
                }
                for (int lhsAttr = 0; lhsAttr < nextSetBit; lhsAttr++) {
                    OpenBitSet lhs = currentLhs.clone();
                    lhs.fastSet(lhsAttr);
                    nextLevel.add(lhs);
                }
            }
            currentLevel = nextLevel;
        }
    }

    private boolean isMaximal(OpenBitSetFD nonFd) {
        return !invalidFDs.containsFdOrGeneralization(nonFd);
    }

    private boolean isValidFd(OpenBitSetFD fd) {
        return validFds.containsFdOrGeneralization(fd);
    }
}

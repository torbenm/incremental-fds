package org.mp.naumann.algorithms.fd.incremental.test;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.List;

public class FDValidator extends Validator {

    private final Lattice fds;
    private final Lattice nonFds;
    private float efficiencyThreshold;

    public FDValidator(int numRecords, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, boolean parallel, Lattice fds, Lattice nonFds, float efficiencyThreshold) {
        super(numRecords, compressedRecords, plis, parallel);
        this.fds = fds;
        this.nonFds = nonFds;
        this.efficiencyThreshold = efficiencyThreshold;
    }

    @Override
    protected boolean collectInvalid() {
        return true;
    }

    @Override
    protected boolean collectValid() {
        return false;
    }

    @Override
    protected boolean isTopDown() {
        return true;
    }

    @Override
    protected Lattice getLattice() {
        return fds;
    }

    @Override
    protected Lattice getInverseLattice() {
        return nonFds;
    }

    @Override
    protected boolean switchToSampler(int previousNumInvalidFds, int numInvalidFds, int numValidFds) {
        return isTopDown() && (numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds);
    }

    @Override
    protected List<OpenBitSetFD> generateSpecializations(OpenBitSetFD fd)  {
        OpenBitSet lhs = fd.getLhs();
        int rhs = fd.getRhs();
        List<OpenBitSetFD> specializations = new ArrayList<>();
        for (int extensionAttribute = 0; extensionAttribute < numAttributes; extensionAttribute++) {
            if (rhs == extensionAttribute // AB -> B is trivial
                    || lhs.fastGet(extensionAttribute) // AA -> B is trivial
                    || fds.containsFdOrGeneralization(new OpenBitSetFD(lhs, extensionAttribute)) // if A -> B, then AB -> C cannot be minimal
                    ) {
                continue;
            }
            OpenBitSet specializedLhs = lhs.clone();
            specializedLhs.fastSet(extensionAttribute);
            OpenBitSetFD specialization = new OpenBitSetFD(specializedLhs, rhs);
            specializations.add(specialization);
        }
        return specializations;
    }

    @Override
    protected void validRhs(LatticeElement elem, int rhs) {
        // No-op
    }

    @Override
    protected void invalidRhs(LatticeElement elem, int rhs) {
        elem.removeFd(rhs);
    }
}

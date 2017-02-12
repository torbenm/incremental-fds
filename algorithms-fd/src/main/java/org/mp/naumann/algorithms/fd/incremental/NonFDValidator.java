package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.Lattice;
import org.mp.naumann.algorithms.fd.structures.LatticeElement;

import java.util.ArrayList;
import java.util.List;

public class NonFDValidator extends IncrementalValidator {

    private final Lattice fds;
    private final Lattice nonFds;

    NonFDValidator(int numRecords, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, boolean parallel, Lattice fds, Lattice nonFds) {
        super(numRecords, compressedRecords, plis, parallel);
        this.fds = fds;
        this.nonFds = nonFds;
    }

    @Override
    protected boolean isTopDown() {
        return false;
    }

    @Override
    protected Lattice getLattice() {
        return nonFds;
    }

    @Override
    protected Lattice getInverseLattice() {
        return fds;
    }

    @Override
    protected boolean switchToSampler(int previousNumInvalidFds, int numInvalidFds, int numValidFds) {
        return false;
    }

    @Override
    protected List<OpenBitSet> generateSpecializations(OpenBitSet lhs, int rhs)  {
        List<OpenBitSet> specializations = new ArrayList<>();
        for (int extensionAttribute = 0; extensionAttribute < numAttributes; extensionAttribute++) {
            if (lhs.fastGet(extensionAttribute) // AA -> B is trivial
                    ) {
                continue;
            }
            OpenBitSet specializedLhs = lhs.clone();
            specializedLhs.fastSet(extensionAttribute);
            specializations.add(specializedLhs);
        }
        return specializations;
    }

    @Override
    protected void validRhs(LatticeElement elem, int rhs) {
        elem.removeFd(rhs);
    }

    @Override
    protected void invalidRhs(LatticeElement elem, int rhs) {
        // No-op
    }
}

package org.mp.naumann.algorithms.fd.incremental.test;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.List;

public class NonFDValidator extends Validator {

    private final Lattice validFds;
    private final Lattice invalidFds;

    public NonFDValidator(int numRecords, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, boolean parallel, Lattice validFds, Lattice invalidFds) {
        super(numRecords, compressedRecords, plis, parallel);
        this.validFds = validFds;
        this.invalidFds = invalidFds;
    }

    @Override
    protected boolean collectInvalid() {
        return false;
    }

    @Override
    protected boolean collectValid() {
        return true;
    }

    @Override
    protected boolean isTopDown() {
        return false;
    }

    @Override
    protected Lattice getLattice() {
        return invalidFds;
    }

    @Override
    protected Lattice getInverseLattice() {
        return validFds;
    }

    @Override
    protected List<OpenBitSetFD> generateSpecializations(OpenBitSetFD fd)  {
        OpenBitSet lhs = fd.getLhs().clone();
        int rhs = fd.getRhs();
        List<OpenBitSetFD> specializations = new ArrayList<>();
        for (int extensionAttribute = 0; extensionAttribute < numAttributes; extensionAttribute++) {
            if (lhs.fastGet(extensionAttribute) // AA -> B is trivial
                    ) {
                continue;
            }
            OpenBitSet specializedLhs = lhs.clone();
            specializedLhs.fastSet(extensionAttribute);
            OpenBitSetFD specialization = new OpenBitSetFD(specializedLhs, rhs);
            if (!invalidFds.containsFdOrGeneralization(specialization)) {
                specializations.add(specialization);
            }
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

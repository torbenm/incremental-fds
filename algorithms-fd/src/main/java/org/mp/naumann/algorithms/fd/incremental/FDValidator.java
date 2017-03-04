package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.Lattice;
import org.mp.naumann.algorithms.fd.structures.LatticeElement;

import java.util.ArrayList;
import java.util.List;

public class FDValidator extends IncrementalValidator {

    private final Lattice fds;
    private final Lattice nonFds;
    private float efficiencyThreshold;
    private final IncrementalMatcher matcher;

    FDValidator(int numRecords, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, boolean parallel, Lattice fds, Lattice nonFds, float efficiencyThreshold, IncrementalMatcher matcher) {
        super(numRecords, compressedRecords, plis, parallel);
        this.fds = fds;
        this.nonFds = nonFds;
        this.efficiencyThreshold = efficiencyThreshold;
        this.matcher = matcher;
    }

    @Override
    protected void end(List<IntegerPair> comparisonSuggestions) {
        comparisonSuggestions.forEach(pair -> matcher.match(new OpenBitSet(numAttributes), pair.a(), pair.b()));
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
    protected boolean interrupt(int previousNumInvalidFds, int numInvalidFds, int numValidFds) {
        //TODO improve for incremental case
        return (numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds);
    }

    @Override
    protected List<OpenBitSet> generateSpecializations(OpenBitSet lhs, int rhs)  {
        List<OpenBitSet> specializations = new ArrayList<>();
        for (int extensionAttribute = 0; extensionAttribute < numAttributes; extensionAttribute++) {
            if (rhs == extensionAttribute // AB -> B is trivial
                    || lhs.fastGet(extensionAttribute) // AA -> B is trivial
                    || fds.containsFdOrGeneralization(lhs, extensionAttribute) // if A -> B, then AB -> C cannot be minimal
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
        // No-op
    }

    @Override
    protected void invalidRhs(LatticeElement elem, int rhs) {
        elem.removeFd(rhs);
    }
}

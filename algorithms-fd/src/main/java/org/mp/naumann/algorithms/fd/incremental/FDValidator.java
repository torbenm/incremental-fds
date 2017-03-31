package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.incremental.structures.Lattice;
import org.mp.naumann.algorithms.fd.incremental.structures.LatticeElement;

import java.util.ArrayList;
import java.util.List;

public class FDValidator extends IncrementalValidator<List<IntegerPair>> {

    private final Lattice fds;
    private final Lattice nonFds;
    private final IncrementalMatcher matcher;
    private List<IntegerPair> comparisonSuggestions = new ArrayList<>();

    FDValidator(int numRecords, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, boolean parallel, Lattice fds, Lattice nonFds, float efficiencyThreshold, IncrementalMatcher matcher) {
        super(numRecords, compressedRecords, plis, parallel, efficiencyThreshold);
        this.fds = fds;
        this.nonFds = nonFds;
        this.matcher = matcher;
    }

    @Override
    protected void end() {
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
    protected void receiveResult(ValidationResult result) {
        this.comparisonSuggestions.addAll(result.comparisonSuggestions);
    }

    @Override
    protected List<IntegerPair> interrupt() {
        List<IntegerPair> result = comparisonSuggestions;
        comparisonSuggestions = new ArrayList<>();
        return result;
    }

    @Override
    protected List<OpenBitSet> generateSpecializations(OpenBitSet lhs, int rhs) {
        List<OpenBitSet> specializations = new ArrayList<>();
        for (int extensionAttribute = 0; extensionAttribute < numAttributes; extensionAttribute++) {
            if (rhs == extensionAttribute // AB -> B is trivial
                    || lhs.fastGet(extensionAttribute) // AA -> B is trivial
//                    || fds.containsFdOrGeneralization(lhs, extensionAttribute) // if A -> B, then AB -> C cannot be minimal
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

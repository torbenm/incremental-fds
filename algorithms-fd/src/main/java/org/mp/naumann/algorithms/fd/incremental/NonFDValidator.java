package org.mp.naumann.algorithms.fd.incremental;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.pruning.Violations;
import org.mp.naumann.algorithms.fd.incremental.structures.Lattice;
import org.mp.naumann.algorithms.fd.incremental.structures.LatticeElement;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.List;

public class NonFDValidator extends IncrementalValidator<List<OpenBitSetFD>> {

    private final Lattice fds;
    private final Lattice nonFds;
    private List<OpenBitSetFD> lastValid;
    private Violations violations;

    NonFDValidator(int numRecords, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, boolean parallel, Lattice fds, Lattice nonFds, float efficiencyThreshold, Violations violations) {
        super(numRecords, compressedRecords, plis, parallel, efficiencyThreshold);
        this.fds = fds;
        this.nonFds = nonFds;
        this.violations = violations;
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
    protected void receiveResult(ValidationResult result) {
        lastValid = result.collectedFDs;
        if (pruningStrategies.contains(PruningStrategy.DELETES)) {
            for (ComparisonSugestion comparisonSuggestion : result.comparisonSuggestions) {
                violations.add(comparisonSuggestion.getPair(), comparisonSuggestion.getFd());
            }
        }
    }

    @Override
    protected List<OpenBitSetFD> interrupt() {
        return lastValid;
    }

    @Override
    protected List<OpenBitSet> generateSpecializations(OpenBitSet lhs, int rhs) {
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

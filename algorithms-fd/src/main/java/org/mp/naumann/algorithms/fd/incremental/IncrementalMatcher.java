package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.agreesets.AgreeSetCollection;
import org.mp.naumann.algorithms.fd.incremental.pruning.Violations;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

class IncrementalMatcher {

	private final CompressedRecords compressedRecords;
	private final ValueComparator valueComparator;
	private final AgreeSetCollection agreeSets;
	private final IncrementalFDConfiguration configuration;
	private final Violations violations;

	IncrementalMatcher(CompressedRecords compressedRecords, ValueComparator valueComparator,
		AgreeSetCollection agreeSets, IncrementalFDConfiguration configuration,
		Violations violations) {
		this.compressedRecords = compressedRecords;
		this.valueComparator = valueComparator;
		this.agreeSets = agreeSets;
		this.configuration = configuration;
		this.violations = violations;
	}

	void match(OpenBitSet equalAttrs, int recId1, int recId2) {
		match(equalAttrs, compressedRecords.get(recId1), this.compressedRecords.get(recId2));
		if (configuration.usesPruningStrategy(PruningStrategy.DELETE_ANNOTATIONS)) {
			agreeSets.addAgreeSet(equalAttrs.clone(), recId1, recId2);
		}
		if (configuration.usesPruningStrategy(PruningStrategy.DELETES)) {
			// All 0s cannot be on the right-hand side anymore if 01010101 is the LHS.
			// Thus flipping gives us the full rhs
			OpenBitSet lhs = equalAttrs.clone();
			OpenBitSet fullRhs = lhs.clone();
			fullRhs.flip(0, compressedRecords.getNumAttributes());

			// Now we go through all "not-rhs's" and specialize the pos. cover with them.
			for (int rhs = fullRhs.nextSetBit(0); rhs >= 0; rhs = fullRhs.nextSetBit(rhs + 1)) {
				OpenBitSetFD fd = new OpenBitSetFD(lhs, rhs);
				violations.add(new IntegerPair(recId1, recId2), fd);
			}
		}
	}

	private void match(OpenBitSet equalAttrs, int[] t1, int[] t2) {
		equalAttrs.clear(0, t1.length);
		for (int i = 0; i < t1.length; i++) {
			if (this.valueComparator.isEqual(t1[i], t2[i])) {
				equalAttrs.set(i);
			}
		}
	}
}

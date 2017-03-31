package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.agreesets.AgreeSetCollection;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

class IncrementalMatcher {

    private final CompressedRecords compressedRecords;
    private final ValueComparator valueComparator;
    private final AgreeSetCollection agreeSets;
    private final IncrementalFDConfiguration configuration;

    IncrementalMatcher(CompressedRecords compressedRecords, ValueComparator valueComparator, AgreeSetCollection agreeSets, IncrementalFDConfiguration configuration) {
        this.compressedRecords = compressedRecords;
        this.valueComparator = valueComparator;
        this.agreeSets = agreeSets;
        this.configuration = configuration;
    }

    void match(OpenBitSet equalAttrs, int recId1, int recId2) {
        match(equalAttrs, compressedRecords.get(recId1), this.compressedRecords.get(recId2));
        if (configuration.usesPruningStrategy(PruningStrategy.DELETE_ANNOTATIONS)) {
            agreeSets.addAgreeSet(equalAttrs.clone(), recId1, recId2);
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

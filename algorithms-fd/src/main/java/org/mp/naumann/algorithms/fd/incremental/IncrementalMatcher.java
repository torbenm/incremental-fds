package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.DeletePruner;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

class IncrementalMatcher {

    private final CompressedRecords compressedRecords;
    private final ValueComparator valueComparator;
    private final DeletePruner pruner;
    private final IncrementalFDConfiguration configuration;

    IncrementalMatcher(CompressedRecords compressedRecords, ValueComparator valueComparator, DeletePruner pruner, IncrementalFDConfiguration configuration) {
        this.compressedRecords = compressedRecords;
        this.valueComparator = valueComparator;
        this.pruner = pruner;
        this.configuration = configuration;
    }

    void match(OpenBitSet equalAttrs,  int recId1, int recId2) {
        match(equalAttrs, compressedRecords.get(recId1), this.compressedRecords.get(recId2));
        if (configuration.usesPruningStrategy(PruningStrategy.DELETE_ANNOTATIONS)) {
            pruner.addAgreeSet(equalAttrs.clone(), recId1, recId2);
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

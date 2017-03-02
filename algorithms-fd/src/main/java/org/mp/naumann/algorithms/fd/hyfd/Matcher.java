package org.mp.naumann.algorithms.fd.hyfd;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.DeletePruner;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.MaxSizeViolationSet;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.SimpleDeleteValidationPruner;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolationCollection;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

class Matcher {

    private final int[][] compressedRecords;
    private final ValueComparator valueComparator;
    private final ViolationCollection violationCollection;
    private final IncrementalFDConfiguration configuration;
    private final DeletePruner pruner;

    public Matcher(int[][] compressedRecords, ValueComparator valueComparator, ViolationCollection violationCollection, IncrementalFDConfiguration configuration) {
        this.compressedRecords = compressedRecords;
        this.valueComparator = valueComparator;
        this.violationCollection = violationCollection;
        this.configuration = configuration;
        this.pruner = new DeletePruner(compressedRecords[0].length, () -> new MaxSizeViolationSet(100), SimpleDeleteValidationPruner::new);
    }

    public void match(OpenBitSet equalAttrs, int recId1, int recId2) {
        match(equalAttrs, compressedRecords[recId1], compressedRecords[recId2]);
        if (configuration.usesPruningStrategy(PruningStrategy.DELETE_ANNOTATIONS)) {
            pruner.addAgreeSet(equalAttrs.clone(), recId1, recId2);
        }
        if(configuration.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.ANNOTATION)) {
            violationCollection.add(equalAttrs, recId1, recId2);
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

    public DeletePruner getPruner() {
        return pruner;
    }
}

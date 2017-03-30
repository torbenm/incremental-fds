package org.mp.naumann.algorithms.fd.hyfd;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.agreesets.AgreeSetCollection;
import org.mp.naumann.algorithms.fd.incremental.agreesets.MaxSizeViolationSet;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

class Matcher {

    private final int[][] compressedRecords;
    private final ValueComparator valueComparator;
    private final IncrementalFDConfiguration configuration;
    private final AgreeSetCollection agreeSets;

    Matcher(int[][] compressedRecords, ValueComparator valueComparator,
        IncrementalFDConfiguration configuration) {
        this.compressedRecords = compressedRecords;
        this.valueComparator = valueComparator;
        this.configuration = configuration;
        this.agreeSets = new AgreeSetCollection(() -> new MaxSizeViolationSet(1000));
    }

    public void match(OpenBitSet equalAttrs, int recId1, int recId2) {
        match(equalAttrs, compressedRecords[recId1], compressedRecords[recId2]);
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

    AgreeSetCollection getAgreeSets() {
        return agreeSets;
    }
}

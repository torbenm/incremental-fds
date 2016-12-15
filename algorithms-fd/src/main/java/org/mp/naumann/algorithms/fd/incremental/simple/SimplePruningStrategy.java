package org.mp.naumann.algorithms.fd.incremental.simple;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CardinalitySet;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;

import java.util.List;

public class SimplePruningStrategy {

    private final List<String> columns;

    public SimplePruningStrategy(List<String> columns) {
        this.columns = columns;
    }

    public CardinalitySet getExistingCombinations(CompressedDiff diff) {
        int numAttributes = columns.size();
        CardinalitySet existingCombinations = new CardinalitySet(numAttributes);
        for (int[] insert : diff.getInsertedRecords()) {
            OpenBitSet existingCombination = findExistingCombinations(insert);
            existingCombinations.add(existingCombination);
        }
        return existingCombinations;
    }

    private OpenBitSet findExistingCombinations(int[] compressedRecord) {
        OpenBitSet existingCombination = new OpenBitSet(compressedRecord.length);
        int i = 0;
        for (int clusterId : compressedRecord) {
            if (clusterId > -1) {
                existingCombination.fastSet(i);
            }
            i++;
        }
        return existingCombination;
    }
}

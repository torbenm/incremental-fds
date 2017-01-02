package org.mp.naumann.algorithms.fd.incremental.simple;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CardinalitySet;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.PruningStrategy;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.List;

public class SimplePruningStrategyBuilder {

    private final List<String> columns;

    public SimplePruningStrategyBuilder(List<String> columns) {
        this.columns = columns;
    }

    public PruningStrategy buildStrategy(CompressedDiff diff) {
        int numAttributes = columns.size();
        CardinalitySet existingCombinations = new CardinalitySet(numAttributes);
        for (int[] insert : diff.getInsertedRecords()) {
            OpenBitSet existingCombination = findExistingCombinations(insert);
            existingCombinations.add(existingCombination);
        }
        return new SimplePruningStrategy(existingCombinations);
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

    private static class SimplePruningStrategy implements PruningStrategy {

        private final CardinalitySet existingCombinations;

        private SimplePruningStrategy(CardinalitySet existingCombinations) {
            this.existingCombinations = existingCombinations;
        }

        @Override
        public boolean cannotBeViolated(FDTreeElementLhsPair fd) {
            if(fd.getLhs().cardinality() > existingCombinations.getDepth()) {
                return false;
            }
            for (int i = existingCombinations.getDepth(); i >= (int) fd.getLhs().cardinality(); i--) {
                for (OpenBitSet ex : existingCombinations.getLevel(i)) {
                    if (BitSetUtils.isContained(fd.getLhs(), ex)) {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}

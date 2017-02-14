package org.mp.naumann.algorithms.fd.incremental.pruning.simple;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.pruning.CardinalitySet;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.List;

public class ExistingValuesPruningStrategy {

    private final List<String> columns;

    public ExistingValuesPruningStrategy(List<String> columns) {
        this.columns = columns;
    }

    public ValidationPruner analyzeDiff(CompressedDiff diff) {
        int numAttributes = columns.size();
        CardinalitySet existingValues = new CardinalitySet(numAttributes);
        for (int[] insert : diff.getInsertedRecords().values()) {
            OpenBitSet existingValuesMask = findExistingValues(insert);
            existingValues.add(existingValuesMask);
        }
        return new SimpleValidationPruner(existingValues);
    }

    private OpenBitSet findExistingValues(int[] compressedRecord) {
        OpenBitSet existingValues = new OpenBitSet(compressedRecord.length);
        int i = 0;
        for (int clusterId : compressedRecord) {
            if (clusterId > -1) {
                existingValues.fastSet(i);
            }
            i++;
        }
        return existingValues;
    }

    private static class SimpleValidationPruner implements ValidationPruner {

        private final CardinalitySet existingValues;

        private SimpleValidationPruner(CardinalitySet existingCombinations) {
            this.existingValues = existingCombinations;
        }

        @Override
        public boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet rhs) {
            for (int level = existingValues.getDepth(); level >= lhs.cardinality(); level--) {
                for (OpenBitSet existingValuesMask : existingValues.getLevel(level)) {
                    if (BitSetUtils.isContained(lhs, existingValuesMask)) {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}

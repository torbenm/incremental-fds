package org.mp.naumann.algorithms.fd.incremental.violations.matcher;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.Collection;

public class ExactMatcher implements Matcher {
    @Override
    public boolean match(OpenBitSet attrs, int[] violatingValues, int[] removedValues) {
        int j = 0;

        for (Integer attr : BitSetUtils.iterable(attrs)) {
            if (attr < removedValues.length && violatingValues[j] != removedValues[attr]) {
                return false;

            }
            j++;
        }
        return true;
    }

    @Override
    public boolean match(Collection<Integer> recordIds, Collection<Integer> removedRecords) {
        int recordsBefore = recordIds.size();
        recordIds.removeAll(removedRecords);
        return recordIds.size() <= 1;
    }
}

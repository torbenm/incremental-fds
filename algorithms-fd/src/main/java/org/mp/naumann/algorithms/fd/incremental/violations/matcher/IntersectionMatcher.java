package org.mp.naumann.algorithms.fd.incremental.violations.matcher;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;

import java.util.Collection;

public class IntersectionMatcher implements Matcher {
    @Override
    public boolean match(OpenBitSet attrs, int[] violatingValues, int[] removedValues) {
        int j = 0;
        for (int attr : BitSetUtils.iterable(attrs)) {
            if (attr < removedValues.length && violatingValues[j] == removedValues[attr]) {
                return true;
            }
            j++;
        }
        return false;
    }

    @Override
    public boolean match(Collection<Integer> recordIds, Collection<Integer> removedRecords) {
        return CollectionUtils.intersects(recordIds, removedRecords);
    }
}

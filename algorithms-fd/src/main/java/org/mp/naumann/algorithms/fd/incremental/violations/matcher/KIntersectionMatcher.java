package org.mp.naumann.algorithms.fd.incremental.violations.matcher;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

public class KIntersectionMatcher implements Matcher {

    private final int k;

    public KIntersectionMatcher(int k) {
        this.k = k;
    }

    @Override
    public boolean match(OpenBitSet attrs, int[] violatingValues, int[] removedValues) {
        int j = 0;
        int c = 0;
        boolean matchedOnAll = true;
        for (Integer rhsAttr : BitSetUtils.iterable(attrs)) {
            if (rhsAttr < removedValues.length && violatingValues[j] == removedValues[rhsAttr]) {
                if (++c >= k) {
                    return true;
                } else {
                    matchedOnAll = false;
                }
            }
            j++;
        }
        return c >= k || matchedOnAll;
    }
}

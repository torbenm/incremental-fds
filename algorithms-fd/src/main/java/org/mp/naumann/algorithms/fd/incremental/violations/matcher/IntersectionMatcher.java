package org.mp.naumann.algorithms.fd.incremental.violations.matcher;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

public class IntersectionMatcher implements Matcher {
    @Override
    public boolean match(OpenBitSet attrs, int[] violatingValues, int[] removedValues) {
        int j = 0;

        for(Integer rhsAttr : BitSetUtils.iterable(attrs)){
            if(rhsAttr < removedValues.length && violatingValues[j] == removedValues[rhsAttr]){
                return true;
            }
            j++;
        }
        return false;
    }
}

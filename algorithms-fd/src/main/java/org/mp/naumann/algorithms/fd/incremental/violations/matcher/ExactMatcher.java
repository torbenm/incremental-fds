package org.mp.naumann.algorithms.fd.incremental.violations.matcher;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.PrintUtils;

import java.util.Arrays;

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
}

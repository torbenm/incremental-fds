package org.mp.naumann.algorithms.fd.incremental.violations.matcher;

import org.apache.lucene.util.OpenBitSet;

import java.util.Collection;

public interface Matcher {

    boolean match(OpenBitSet attrs, int[] violatingValues, int[] removedValues);

    default boolean matchFlipped(OpenBitSet attrs, int[] violatingValues, int[] removedValues){
        OpenBitSet flipped = attrs.clone();
        flipped.flip(0, attrs.length());
        return match(flipped, violatingValues, removedValues);
    }

    default boolean match(boolean compareEqual, OpenBitSet attrs, int[] violatingValues, int[] removedValues){
        return compareEqual ? match(attrs, violatingValues, removedValues) : matchFlipped(attrs, violatingValues, removedValues);
    }


    default boolean match(Collection<Integer> recordIds, Collection<Integer> removedRecords){
        return false;
    }

}

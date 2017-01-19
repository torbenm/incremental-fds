package org.mp.naumann.algorithms.fd.incremental.violations.matcher;

import org.apache.lucene.util.OpenBitSet;

public interface Matcher {

    boolean match(OpenBitSet attrs, int[] violatingValues, int[] removedValues);

}

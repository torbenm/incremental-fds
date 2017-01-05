package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDSet;

import java.util.List;

public interface ViolationCollection {

    void add(OpenBitSet attrs, List<Integer> violatingValues);

    void injectIntoNegativeCover(FDSet negCoverBase);

    void remove(List<Integer> values);

}

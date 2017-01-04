package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;

import java.util.List;

public interface ViolationCollection {

    void addViolationOfNegativeCover(OpenBitSet attrs, List<Integer> violatingValues);
    void addViolationOfFunctionDependency();

}

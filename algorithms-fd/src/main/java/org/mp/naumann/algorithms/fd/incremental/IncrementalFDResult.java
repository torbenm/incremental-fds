package org.mp.naumann.algorithms.fd.incremental;

import org.mp.naumann.algorithms.fd.FunctionalDependency;

import java.util.List;

public class IncrementalFDResult {

    private final int validationCount, prunedCount;
    private final List<FunctionalDependency> fds;

    public IncrementalFDResult(List<FunctionalDependency> fds, int validationCount, int prunedCount) {
        this.fds = fds;
        this.validationCount = validationCount;
        this.prunedCount = prunedCount;
    }

    public int getValidationCount() {
        return validationCount;
    }

    public int getPrunedCount() {
        return prunedCount;
    }

    public List<FunctionalDependency> getFDs() {
        return fds;
    }

}

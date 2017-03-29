package org.mp.naumann.algorithms.fd.utils;

import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDResult;
import org.mp.naumann.algorithms.result.ResultListener;

import java.util.List;
import java.util.logging.Level;

public class IncrementalFDResultListener implements ResultListener<IncrementalFDResult> {

    private static int validationCount = 0, prunedCount = 0;
    private static List<FunctionalDependency> fds = null;

    @Override
    public void receiveResult(IncrementalFDResult result) {
        fds = result.getFDs();
        FDLogger.log(Level.INFO, String.format("New FD count: %s", fds.size()));
        FDLogger.log(Level.INFO, String.format("Validations made: %s", result.getValidationCount()));
        FDLogger.log(Level.INFO, String.format("Pruned validations: %s", result.getPrunedCount()));
        fds.forEach(fd -> FDLogger.log(Level.FINEST, fd.toString()));
        validationCount += result.getValidationCount();
        prunedCount += result.getPrunedCount();
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

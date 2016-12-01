package org.mp.naumann.algorithms.fd.utils;

import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.result.ResultListener;

import java.util.List;
import java.util.logging.Level;

public class FDListResultListener implements ResultListener<List<FunctionalDependency>> {

    @Override
    public void receiveResult(List<FunctionalDependency> result) {
        FDLogger.log(Level.INFO, String.format("New FD count: %s", result.size()));
        result.forEach(fd -> FDLogger.log(Level.FINER, fd.toString()));
    }
}

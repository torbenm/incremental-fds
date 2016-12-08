package org.mp.naumann.algorithms.fd.fdep;

import org.mp.naumann.algorithms.fd.FDAlgorithmTest;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;

import java.util.logging.Level;

public class FDEPExecutorTest extends FDAlgorithmTest {

    @Override
    protected FunctionalDependencyAlgorithm getNewInstance() {
        FDLogger.setLevel(Level.OFF);
        return new FDEPExecutor();
    }
}
package org.mp.naumann.algorithms.fd.fdep;

import org.junit.After;
import org.junit.Before;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDAlgorithmTest;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmFixture;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.InputReadException;

public class FDEPExecutorTest extends FDAlgorithmTest {

    @Override
    protected FunctionalDependencyAlgorithm getNewInstance() {
        FDLogger.silence();
        return new FDEPExecutor();
    }
}
package org.mp.naumann.algorithms.fd.hyfd;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.exceptions.ColumnNameMismatchException;
import org.mp.naumann.algorithms.exceptions.CouldNotReceiveResultException;
import org.mp.naumann.algorithms.fd.FDAlgorithmTest;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmFixture;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.InputReadException;

public class HyFDTest extends FDAlgorithmTest {


	@Override
	protected FunctionalDependencyAlgorithm getNewInstance() {
		return new HyFD();
	}
}

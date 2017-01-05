package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;

import java.util.List;
import java.util.logging.Level;

public class FDDemo {

	public static void main(String[] args) throws ClassNotFoundException, ConnectionException {
		FDLogger.setLevel(Level.OFF);
		SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(System.out::println);
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection("/", ","));
		SpeedBenchmark.lap(BenchmarkLevel.ALGORITHM, "Loaded dataconnector");
		InitialAlgorithm<List<FunctionalDependency>, ?> hyfd = new FDInitialAlgorithm("hyfd", dc, "test", "deletesample.result");
		List<FunctionalDependency> fds = hyfd.execute();
		SpeedBenchmark.end(BenchmarkLevel.ALGORITHM,"Finished execution "+fds.size());
		fds.forEach(System.out::println);
	}

}

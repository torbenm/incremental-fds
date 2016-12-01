package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;

import java.util.List;

public class FDDemo {

	public static void main(String[] args) throws ClassNotFoundException, ConnectionException {
		FDLogger.silence();
		SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(System.out::println);
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection("/test", ";"));
		SpeedBenchmark.lap(BenchmarkLevel.ALGORITHM, "Loaded dataconnector");
		InitialAlgorithm<List<FunctionalDependency>, ?> hyfd = new FDInitialAlgorithm("hyfd", dc, "test", "countries");
		List<FunctionalDependency> fds = hyfd.execute();
		SpeedBenchmark.end(BenchmarkLevel.ALGORITHM,"Finished execution");
		//fds.forEach(System.out::println);
	}

}

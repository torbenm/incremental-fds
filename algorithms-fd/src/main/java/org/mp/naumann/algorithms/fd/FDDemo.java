package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;

import java.util.List;
import java.util.logging.Level;

import ResourceConnection.ResourceConnector;

public class FDDemo {

	public static void main(String[] args) throws ClassNotFoundException, ConnectionException {
		FDLogger.setLevel(Level.OFF);
		SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(System.out::println);
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection(ResourceConnector.TEST, ";"));
		SpeedBenchmark.lap(BenchmarkLevel.ALGORITHM, "Loaded dataconnector");
		InitialAlgorithm<List<FunctionalDependency>, ?> hyfd = new FDInitialAlgorithm("hyfd", dc,
                "benchmark",
                "adult.deleted",
                //"",
           //     "test.deletesample.result"
            //   "test.bridges.result"
                IncrementalFDConfiguration.LATEST);
		List<FunctionalDependency> fds = hyfd.execute();
		SpeedBenchmark.end(BenchmarkLevel.ALGORITHM,"Finished execution "+fds.size());
		fds.forEach(System.out::println);
        System.out.println("Found "+fds.size());
	}

}

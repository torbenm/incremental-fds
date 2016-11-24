package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.Speed;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;

import java.util.List;

public class FDDemo {

	public static void main(String[] args) throws ClassNotFoundException, ConnectionException {
		FDLogger.silence();
		Speed s = new Speed(System.out::println);
		s.start("Start HyFD execution on countries dataset.");
        DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection("/test", ";"));
		Speed.lap("Loaded dataconnector");
		InitialAlgorithm<List<FunctionalDependency>, ?> hyfd = new FDInitialAlgorithm("hyfd", dc, "test", "countries");
		List<FunctionalDependency> fds = hyfd.execute();
		s.end("Finished execution");
		//fds.forEach(System.out::println);
	}

}

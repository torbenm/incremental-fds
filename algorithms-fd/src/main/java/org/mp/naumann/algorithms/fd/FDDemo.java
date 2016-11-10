package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.PostgresConnection;

import java.util.List;

public class FDDemo {

	public static void main(String[] args) {
        DataConnector dc = new JdbcDataConnector("org.postgresql.Driver", PostgresConnection.getConnectionInfo());
		InitialAlgorithm<List<FunctionalDependency>, ?> hyfd = new FDInitialAlgorithm("hyfd", dc, "test", "countries");
		List<FunctionalDependency> fds = hyfd.execute();
		fds.forEach(System.out::println);
	}

}

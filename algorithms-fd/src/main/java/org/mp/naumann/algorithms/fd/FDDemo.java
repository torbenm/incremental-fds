package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.result.ResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.PostgresConnection;

import java.util.logging.LogManager;
import java.util.logging.Logger;

public class FDDemo {

	public static void main(String[] args) {
        DataConnector dc = new JdbcDataConnector("org.postgresql.Driver", PostgresConnection.getConnectionInfo());
		InitialAlgorithm<FunctionalDependency, ?> hyfd = new FDInitialAlgorithm("hyfd", dc, "test", "countries");
		ResultSet<FunctionalDependency> fds = hyfd.execute().getResultSet();
		fds.forEach(System.out::println);
	}

}

package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.result.ResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;

import java.util.logging.LogManager;
import java.util.logging.Logger;

public class FDDemo {

	public static void main(String[] args) {

        DataConnector dc = new JdbcDataConnector("org.relique.jdbc.csv.CsvDriver", "jdbc:relique:csv:algorithms-fd/src/test/data");
		InitialAlgorithm<FunctionalDependency, ?> hyfd = new FDInitialAlgorithm("hyfd", dc, "", "test");
		ResultSet<FunctionalDependency> fds = hyfd.execute().getResultSet();
		fds.forEach(System.out::println);
	}

}

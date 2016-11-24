package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.fd.incremental.IncrementalFD;
import org.mp.naumann.algorithms.result.DefaultResultListener;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.ListBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IncrementalFDDemo {

	public static void main(String[] args) throws ClassNotFoundException, ConnectionException {
		try (DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection("../test_data"))) {
			String tableName = "data";
			String schema = "public";
			Table table = dc.getTable(schema, tableName);
			HyFDInitialAlgorithm hyfd = new HyFDInitialAlgorithm(table);
			List<FunctionalDependency> fds = hyfd.execute();
//			fds.forEach(System.out::println);
			FDIntermediateDatastructure ds = hyfd.getIntermediateDataStructure();
			
			IncrementalFD inc = new IncrementalFD(Arrays.asList("a", "b", "c", "d"), tableName);
			DefaultResultListener<List<FunctionalDependency>> listener = new DefaultResultListener<>();
			inc.addResultListener(listener);
			inc.setIntermediateDataStructure(ds);
			
			List<Statement> statements = new ArrayList<>();
			Map<String, String> map = new HashMap<>();
			map.put("a", "2");
			map.put("b", "3");
			map.put("c", "1");
			map.put("d", "4");
			statements.add(new DefaultInsertStatement(map, schema, tableName));
			Batch batch = new ListBatch(statements, schema, tableName);
			inc.handleBatch(batch);
			
//			listener.getResult().forEach(System.out::println);
		}
	}

}

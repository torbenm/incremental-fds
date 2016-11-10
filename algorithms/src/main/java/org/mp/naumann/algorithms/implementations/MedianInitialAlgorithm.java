package org.mp.naumann.algorithms.implementations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.result.AlgorithmResult;
import org.mp.naumann.algorithms.result.SimpleObjectResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

public class MedianInitialAlgorithm extends InitialAlgorithm {

	private String column;
	private String table;

	public MedianInitialAlgorithm(DataConnector dataConnector, String table, String column) {
		super(dataConnector);
		this.column = column;
		this.table = table;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	@Override
	public AlgorithmResult execute() {
		Table t = getDataConnector().getTable(table);
		try (TableInput input = t.open()) {
			return executeAlgorithm(input);
		} catch (InputReadException e) {
			throw new RuntimeException(e);
		}
	}

	protected AlgorithmResult executeAlgorithm(TableInput input) {
		List<String> values = new ArrayList<>();
		while (input.hasNext()) {
			Row row = input.next();
			values.add(row.getValue(column));
		}
		Collections.sort(values);
		AlgorithmResult result = new AlgorithmResult();
		result.setResultSet(new SimpleObjectResultSet(values.get(values.size() / 2)));
		return result;
	}
}

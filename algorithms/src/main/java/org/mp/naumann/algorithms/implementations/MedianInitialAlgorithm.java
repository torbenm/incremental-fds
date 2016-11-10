package org.mp.naumann.algorithms.implementations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.data.NoIntermediateDataStructure;
import org.mp.naumann.algorithms.result.AlgorithmResult;
import org.mp.naumann.algorithms.result.SingleResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

public class MedianInitialAlgorithm extends InitialAlgorithm<String, NoIntermediateDataStructure> {

	private String column, schema, table;

	public MedianInitialAlgorithm(DataConnector dataConnector, String schema, String table, String column) {
		super(dataConnector);
		this.column = column;
		this.schema = schema;
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

	public String getSchema() { return schema; }

	public void setSchema(String Schema) { this.schema = Schema; }

	@Override
	public AlgorithmResult<String, NoIntermediateDataStructure> execute() {
		Table t = getDataConnector().getTable(schema, table);
		try (TableInput input = t.open()) {
			return executeAlgorithm(input);
		} catch (InputReadException e) {
			throw new RuntimeException(e);
		}
	}

	protected AlgorithmResult<String, NoIntermediateDataStructure> executeAlgorithm(TableInput input) {
		List<String> values = new ArrayList<>();
		while (input.hasNext()) {
			Row row = input.next();
			values.add(row.getValue(column));
		}
		Collections.sort(values);
		AlgorithmResult<String, NoIntermediateDataStructure> result = new AlgorithmResult<>();
		result.setResultSet(new SingleResultSet<>(values.get(values.size() / 2)));
		return result;
	}
}

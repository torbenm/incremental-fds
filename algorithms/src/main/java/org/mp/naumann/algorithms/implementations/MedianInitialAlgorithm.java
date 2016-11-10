package org.mp.naumann.algorithms.implementations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

public class MedianInitialAlgorithm implements InitialAlgorithm<String, TreeSet<String>> {

	private String column;
	private String table;
	private DataConnector dataConnector;
	private TreeSet<String> tree;

	public MedianInitialAlgorithm(DataConnector dataConnector, String table, String column) {
		this.dataConnector = dataConnector;
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
	public String execute() {
		Table t = dataConnector.getTable(table);
		try (TableInput input = t.open()) {
			return executeAlgorithm(input);
		} catch (InputReadException e) {
			throw new RuntimeException(e);
		}
	}

	protected String executeAlgorithm(TableInput input) {
		List<String> values = new ArrayList<>();
		while (input.hasNext()) {
			Row row = input.next();
			values.add(row.getValue(column));
		}
		Collections.sort(values);
		tree = new TreeSet<>(values);
		return values.get(values.size() / 2);
	}

	@Override
	public TreeSet<String> getIntermediateDataStructure() {
		return tree;
	}
}

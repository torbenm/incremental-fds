package org.mp.naumann.database.data;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GenericRow implements Row {

	private final Map<String, String> values;
	private final List<Column<String>> columns;

	public GenericRow(Map<String, String> values, List<Column<String>> columns) {
		this.values = values;
		this.columns = columns;
	}

	@Override
	public String getValue(int columnIndex) {
		return getValue(columns.get(columnIndex));
	}

	@Override
	public Map<String, String> getValues() {
		return values;
	}

	@Override
	public List<Column<String>> getColumns() {
		return columns;
	}

	@Override
	public Iterator<String> iterator() {
		return columns.stream().map(this::getValue).iterator();
	}
}

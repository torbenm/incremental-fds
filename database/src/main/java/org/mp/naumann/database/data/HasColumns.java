package org.mp.naumann.database.data;

import java.util.List;
import java.util.stream.Collectors;

public interface HasColumns {

	default int numberOfColumns() {
		return getColumns().size();
	}

	List<Column<String>> getColumns();

	default List<String> getColumnNames() {
		return getColumns().stream().map(Column::getName).collect(Collectors.toList());
	}

	default Column<String> getColumn(String columnName) {
		for (Column<String> column : getColumns()) {
			if (column.getName().equals(columnName)) {
				return column;
			}
		}
		return null;
	}
}

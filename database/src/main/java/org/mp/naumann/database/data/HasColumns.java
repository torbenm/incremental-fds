package org.mp.naumann.database.data;

import java.util.List;
import java.util.stream.Collectors;

public interface HasColumns<T> {

    default int numberOfColumns() {
        return getColumns().size();
    }

    List<Column<T>> getColumns();

    default List<String> getColumnNames() {
        return getColumns().stream().map(Column::getName).collect(Collectors.toList());
    }

    default Column<T> getColumn(String columnName) {
        for (Column<T> column : getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        return null;
    }
}

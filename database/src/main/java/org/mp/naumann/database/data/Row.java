package org.mp.naumann.database.data;

import java.util.Map;

public interface Row extends HasColumns<String>, Iterable<String> {

    default String getValue(String columnName) {
        return getValues().get(columnName);
    }

    default String getValue(Column<String> column) {
        return getValue(column.getName());
    }

    String getValue(int columnIndex);


    Map<String, String> getValues();

}

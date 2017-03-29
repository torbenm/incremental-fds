package org.mp.naumann.database.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GenericRow implements Row {

    private final Map<String, String> values;
    private final List<Column<String>> columns;

    public GenericRow(Map<String, String> values, List<Column<String>> columns) {
        this.values = values;
        this.columns = columns;
    }

    public static GenericRow of(final List<Column<String>> columns, String... values) {
        HashMap<String, String> valueMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            valueMap.put(columns.get(i).getName(), values[i]);
        }
        return new GenericRow(valueMap, columns);
    }

    public static GenericRow ofColumnNames(final List<String> columnNames, String... values) {
        List<Column<String>> columns = columnNames.parallelStream().map(GenericColumn::StringColumn).collect(Collectors.toList());
        return of(columns, values);
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

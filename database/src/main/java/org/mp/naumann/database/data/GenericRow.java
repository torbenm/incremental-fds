package org.mp.naumann.database.data;

import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GenericRow implements Row {

    private Map<String, Object> values;
    private RowIdentifier rowIdentifier;

    public GenericRow(RowIdentifier rowIdentifier, Map<String, Object> values) {
        this.rowIdentifier = rowIdentifier;
        this.values = values;
    }

    public List<String> getColumnNames() {
        return new ArrayList<>(values.keySet());
    }

    public List<Object> toList() {
        return new ArrayList<>(values.values());
    }

    @Override
    public Map<String, Object> getValues() {
        return values;
    }

    public Object getValue(String id) { return values.get(id); }

    public RowIdentifier getRowIdentifier() { return rowIdentifier; }
}

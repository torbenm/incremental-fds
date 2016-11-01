package org.mp.naumann.database;

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
        return new ArrayList<String>(values.keySet());
    }

    public List<Object> toList() {
        return new ArrayList<Object>(values.values());
    }

    public Object getValue(String id) { return values.get(id); }

    public RowIdentifier getRowIdentifier() { return rowIdentifier; }
}

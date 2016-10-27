package org.mp.naumann.database;

import org.mp.naumann.database.identifier.DefaultRowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GenericColumn<T> implements Column<T> {

    private String name;
    private Map<RowIdentifier, T> values;

    public GenericColumn(String name, Map<RowIdentifier, T> values) {

        this.name = name;
        this.values = values;
        System.out.println(values.get(new DefaultRowIdentifier(1)).getClass().toString());
    }

    public String getName() {
        return name;
    }

    public List<T> toList() {
        if (values.values() instanceof List)
            return (List<T>) values.values();
        else
            return new ArrayList<>(values.values());
    }

    public T getValue(RowIdentifier id) { return values.get(id); }

}

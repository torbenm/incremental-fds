package org.mp.naumann.database.data;

import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GenericColumn<T> implements Column<T> {

    private String name;
    private Map<RowIdentifier, T> values;

    public GenericColumn(String name, Map<RowIdentifier, T> values) {
        this.name = name;
        this.values = values;
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

    public Class<T> getColumnType() {
        Optional<T> o = values.values().stream().findFirst();
        if (o.isPresent())
            //noinspection unchecked
            return (Class<T>)o.get().getClass();
        else
            return null;
    }
}

package org.mp.naumann.database.data;

import org.mp.naumann.database.identifier.RowIdentifier;

public interface Column<T> extends ValueContainer<T, RowIdentifier>, HasName {

    Class<T> getColumnType();
}

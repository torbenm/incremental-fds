package org.mp.naumann.database;

import org.mp.naumann.database.identifier.RowIdentifier;

public interface Column<T> extends ValueContainer<T, RowIdentifier>, HasName {
}

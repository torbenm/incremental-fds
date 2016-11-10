package org.mp.naumann.database.data;

public interface Column<T> extends HasName {

	Class<T> getType();
}

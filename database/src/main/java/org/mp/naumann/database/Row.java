package org.mp.naumann.database;

public interface Row<T> extends HasColumns, HasName, HasRowIdentifier, ValueContainer<T, String> {

}

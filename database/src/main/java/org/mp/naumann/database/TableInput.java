package org.mp.naumann.database;

import java.util.Iterator;

import org.mp.naumann.database.data.HasColumns;
import org.mp.naumann.database.data.HasName;
import org.mp.naumann.database.data.Row;

public interface TableInput extends AutoCloseable, Iterator<Row>, HasColumns<String>, HasName {
	
	void close() throws InputReadException;

}

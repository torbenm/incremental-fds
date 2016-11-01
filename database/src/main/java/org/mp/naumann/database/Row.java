package org.mp.naumann.database;

import org.mp.naumann.database.identifier.HasRowIdentifier;

public interface Row extends HasColumns, HasRowIdentifier, ValueContainer<Object, String> {

}

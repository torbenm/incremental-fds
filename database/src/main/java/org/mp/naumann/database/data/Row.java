package org.mp.naumann.database.data;

import org.mp.naumann.database.identifier.HasRowIdentifier;

public interface Row extends HasColumns, HasRowIdentifier, ValueContainer<Object, String> {

}

package org.mp.naumann.database.data;

import java.util.Map;

public interface Row extends HasColumns {

	default String getValue(String columnName) {
		return getValues().get(columnName);
	}

	Map<String, String> getValues();

}

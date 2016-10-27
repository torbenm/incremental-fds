package org.mp.naumann.database.statement;

import java.util.Map;

public interface WriteStatement<T> extends Statement {

    Map<String, T> getValueMap();
}

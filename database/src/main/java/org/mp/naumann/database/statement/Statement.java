package org.mp.naumann.database.statement;

import java.util.Map;

public interface Statement {

    String getTableName();
    String getSchema();
    Map<String, String> getValueMap();
}

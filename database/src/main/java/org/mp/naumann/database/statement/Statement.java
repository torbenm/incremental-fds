package org.mp.naumann.database.statement;

import java.util.Map;

public interface Statement {

    String getTableName();
    Map<String, String> getValueMap();
}

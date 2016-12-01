package org.mp.naumann.database.statement;

import java.sql.JDBCType;
import java.util.Map;

public interface Statement {

    String getTableName();
    String getSchema();
    Map<String, String> getValueMap();
    default JDBCType getJDBCType(String columnName) { return JDBCType.VARCHAR; }
    boolean isOfEqualLayout(Statement statement);
}

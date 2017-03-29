package org.mp.naumann.database.statement;

import java.sql.JDBCType;

public interface Statement {

    String getTableName();

    String getSchema();

    default JDBCType getJDBCType(String columnName) {
        return JDBCType.VARCHAR;
    }

    void accept(StatementVisitor visitor);

    default boolean isOfEqualLayout(Statement statement) {
        return false;
    }

    boolean isEmpty();
}

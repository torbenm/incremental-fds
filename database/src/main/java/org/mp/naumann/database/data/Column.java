package org.mp.naumann.database.data;

import java.sql.JDBCType;

public interface Column<T> extends HasName {

    Class<T> getType();

    default JDBCType getJDBCType() {
        return JDBCType.VARCHAR;
    }
}

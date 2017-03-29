package org.mp.naumann.database.data;

import java.sql.JDBCType;

public class StringColumn implements Column<String> {

    private final String name;
    private final JDBCType jdbcType;

    public StringColumn(String name) {
        this(name, JDBCType.VARCHAR);
    }

    public StringColumn(String name, JDBCType jdbcType) {
        this.name = name;
        this.jdbcType = jdbcType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Class<String> getType() {
        return String.class;
    }

    @Override
    public JDBCType getJDBCType() {
        return jdbcType;
    }

}

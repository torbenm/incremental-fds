package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Column;
import org.mp.naumann.database.data.StringColumn;
import org.mp.naumann.database.jdbc.sql.SqlQueryBuilder;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class JdbcTable implements Table {

    private Connection conn;
    private String schema, name, fullName;
    private List<Column<String>> columns;
    private int limit = -1;

    JdbcTable(String schema, String name, Connection conn) {
        this.schema = schema;
        this.name = name;
        this.conn = conn;
        fullName = schema.equals("") ? name : schema + "." + name;
    }

    @Override
    public boolean execute(Statement statement) {
        try (java.sql.Statement stmt = conn.createStatement()) {
            return stmt.execute(SqlQueryBuilder.generateSql(statement));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean execute(StatementGroup statementGroup) {
        try (java.sql.Statement stmt = conn.createStatement()) {
            return stmt.execute(SqlQueryBuilder.generateSql(statementGroup));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public int numberOfColumns() {
        return getColumns().size();
    }

    @Override
    public List<Column<String>> getColumns() {
        if (columns == null) {
            try {
                columns = new ArrayList<>();
                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet rs = meta.getColumns(null, schema, name, null)) {
                    while (rs.next()) {
                        columns.add(new StringColumn(rs.getString(4), JDBCType.valueOf(rs.getInt(5))));
                    }
                }

                // workaround because the csvdriver needs tablenames as "schema.table" (probably a bug?)
                if (columns.isEmpty()) {
                    try (ResultSet rs = meta.getColumns(null, schema, schema + "." + name, null)) {
                        while (rs.next()) {
                            String name = rs.getString(4);
                            columns.add(new StringColumn(name, JDBCType.valueOf(rs.getInt(5))));
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to retrieve columns", e);
            }
        }
        return columns;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getRowCount() {
        try (java.sql.Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", fullName))) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException ignored) {
        }
        return -1;
    }

    @Override
    public TableInput open() throws InputReadException {
        try {
            String sql = String.format("SELECT * FROM %s", fullName);
            if (limit > 0) {
                sql += " LIMIT " + limit;
            }
            ResultSet rs = conn.createStatement().executeQuery(sql);
            return new JdbcTableInput(rs, name);
        } catch (SQLException e) {
            throw new InputReadException(e);
        }
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

}

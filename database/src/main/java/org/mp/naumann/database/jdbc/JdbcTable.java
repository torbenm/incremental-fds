package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.*;
import org.mp.naumann.database.data.Column;
import org.mp.naumann.database.data.GenericColumn;
import org.mp.naumann.database.data.GenericRow;
import org.mp.naumann.database.data.Row;
import org.mp.naumann.database.identifier.DefaultRowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifierGroup;
import org.mp.naumann.database.jdbc.sql.SqlQueryBuilder;
import org.mp.naumann.database.jdbc.sql.SqlTypeMap;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;
import static org.mp.naumann.utils.GenericHelper.cast;
import static org.mp.naumann.utils.GenericHelper.createGenericMap;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class JdbcTable implements Table {

    private String name;
    private Connection conn;
    private Class<? extends RowIdentifier> rowIdentifierType;

    JdbcTable(String name, Connection conn) {
        this.name = name;
        this.conn = conn;
        rowIdentifierType = determineRowIdentifierType();
    }

    private Class<? extends RowIdentifier> determineRowIdentifierType() {
        return DefaultRowIdentifier.class;
    }

    public List<String> getColumnNames() {
        List<String> result = new ArrayList<>();
        try {
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getColumns(null, null, name, null)) {
                while (rs.next()) {
                    result.add(rs.getString(4));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public long getRowCount() {
        try (java.sql.Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", name))) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public Row getRow(RowIdentifier rowIdentifier) {
        try (java.sql.Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY 1 LIMIT 1 OFFSET %s", this.name, rowIdentifier.getRowId()))) {
                ResultSetMetaData md = rs.getMetaData();
                Map<String, Object> values = new HashMap<>();
                if (rs.next()) {
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        values.put(md.getColumnName(i), rs.getObject(i));
                    }
                }
                if (values.size() > 0)
                    return new GenericRow(rowIdentifier, values);
                else
                    return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Column getColumn(String name) {
        try (java.sql.Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(String.format("SELECT %s FROM %s", name, this.name))) {
                Class<?> columnType = SqlTypeMap.toClass(rs.getMetaData().getColumnType(1));
                Map values = createGenericMap(HashMap.class, RowIdentifier.class, columnType);
                int i = 0;
                while (rs.next()) {
                    //noinspection unchecked
                    values.put(new DefaultRowIdentifier(i), cast(rs.getObject(1), columnType));
                    i++;
                }
                //noinspection unchecked
                return new GenericColumn(name, values);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean execute(Statement statement) {
        try (java.sql.Statement stmt = conn.createStatement()) {
            return stmt.execute(SqlQueryBuilder.generateSql(statement));
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean execute(StatementGroup statementGroup) {
        try(java.sql.Statement stmt = conn.createStatement()){
            return stmt.execute(SqlQueryBuilder.generateSql(statementGroup));
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Table getSubTable(RowIdentifierGroup group) {
        return null;
    }

    public String getName() {
        return name;
    }

    public Class<? extends RowIdentifier> getRowIdentifierType() { return rowIdentifierType; }

}

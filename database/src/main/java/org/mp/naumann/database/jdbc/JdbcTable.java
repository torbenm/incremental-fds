package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.Column;
import org.mp.naumann.database.GenericColumn;
import org.mp.naumann.database.Row;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.identifier.DefaultRowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifierGroup;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;
import static org.mp.naumann.utils.GenericHelper.cast;
import static org.mp.naumann.utils.GenericHelper.createGenericMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcTable implements Table {

    private String name;
    private Connection conn;
    private Class<? extends RowIdentifier> rowIdentifierType;

    public JdbcTable(String name, Connection conn) {
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
        return null;
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
        return false;
    }

    public boolean execute(StatementGroup statementGroup) {
        return false;
    }

    public Table getSubTable(RowIdentifierGroup group) {
        return null;
    }

    public String getName() {
        return name;
    }

    public Class<? extends RowIdentifier> getRowIdentifierType() { return rowIdentifierType; };

}

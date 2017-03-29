package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Column;
import org.mp.naumann.database.data.GenericRow;
import org.mp.naumann.database.data.Row;
import org.mp.naumann.database.data.StringColumn;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class JdbcTableInput implements TableInput {

    private final ResultSet rs;
    private final String name;
    private Boolean hasNext = null;
    private List<Column<String>> columns;

    public JdbcTableInput(ResultSet rs, String relationName) {
        this.rs = rs;
        this.name = relationName;
    }

    public Row currentRow() {
        try {
            Map<String, String> values = new HashMap<>();
            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                String value = rs.getString(i);
                values.put(metaData.getColumnName(i), (rs.wasNull() ? "" : value));
            }
            return new GenericRow(values, getColumns());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws InputReadException {
        try {
            rs.close();
        } catch (SQLException e) {
            throw new InputReadException(e);
        }
    }

    @Override
    public boolean hasNext() {
        if (hasNext == null) {
            try {
                hasNext = rs.next();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return hasNext;
    }

    @Override
    public Row next() {
        if (hasNext == null) {
            hasNext();
        }
        if (hasNext) {
            hasNext = null;
            return currentRow();
        }
        throw new NoSuchElementException();
    }

    @Override
    public List<Column<String>> getColumns() {
        if (columns == null) {
            try {
                columns = new ArrayList<>();
                ResultSetMetaData meta = rs.getMetaData();
                int count = meta.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    columns.add(new StringColumn(meta.getColumnName(i), JDBCType.valueOf(meta.getColumnType(i))));
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

}

package org.mp.naumann.database;

import org.mp.naumann.database.identifier.DefaultRowIdentifier;
import org.mp.naumann.database.jdbc.JdbcDataConnector;

public class DatabaseTest {

    public static void main(String[] args) {
        DataConnector connector = new JdbcDataConnector("org.relique.jdbc.csv.CsvDriver", "jdbc:relique:csv:database/src/main/resources");
        try {
            for (String tableName : connector.getTableNames()) {
                System.out.println(String.format("Table name: %s", tableName));
                Table table = connector.getTable(tableName);
                System.out.println(String.format("Row count: %s", table.getRowCount()));
                System.out.println("Columns:");
                for (String columnName: table.getColumnNames()) {
                    Column col = table.getColumn(columnName);
                    System.out.println(String.format("    %s (%s)", col.getName(), col.getColumnType()));
                }
            }
        } finally {
            connector.disconnect();
        }

    }
}

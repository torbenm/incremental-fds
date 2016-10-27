package org.mp.naumann.database;

import org.mp.naumann.database.identifier.DefaultRowIdentifier;
import org.mp.naumann.database.jdbc.JDBCDataConnector;

public class DatabaseTest {

    public static void main(String[] args) {
        DataConnector connector = new JDBCDataConnector("org.relique.jdbc.csv.CsvDriver", "jdbc:relique:csv:database/src/main/resources");
        try {
            for (String tableName : connector.getTableNames()) {
                System.out.println(String.format("Table name: %s", tableName));
                Table table = connector.getTable(tableName);
                System.out.println(String.format("Row count: %s", table.getRowCount()));
                System.out.println("Column names:");
                for (String columnName: table.getColumnNames()) {
                    Column<Object> col = table.getColumn(columnName);
                    System.out.println(String.format("    %s", col.getName()));
                    System.out.println(String.format("    %s", col.getValue(new DefaultRowIdentifier(1))));
                }
            }
        } finally {
            connector.disconnect();
        }

    }
}

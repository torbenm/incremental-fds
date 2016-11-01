package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.Map;

class SqlQueryBuilder {

    static String generateSql(Statement stmt){
        if (stmt instanceof InsertStatement) {
            return generateSql((InsertStatement)stmt);
        }

        if (stmt instanceof DeleteStatement) {
            return generateSql((DeleteStatement) stmt);
        }

        if (stmt instanceof UpdateStatement) {
            return generateSql((UpdateStatement) stmt);
        }

        return "ERROR! However, this case should never occur.";
    }

    private static void appendValueMapToStringBuilder(Map<String, String> valueMap, StringBuilder query, String separator) {
        if (valueMap.isEmpty()) {
            query.append("1 = 0");
        } else {
            valueMap.forEach(
                    (key, value) -> query
                            .append(key)
                            .append(" = ")
                            .append(value)
                            .append(separator)
            );
            query.setLength(query.length() - separator.length());
        }
    }

    private static String generateSql(InsertStatement stmt){
        return "INSERT INTO "
            + stmt.getTableName()
            + " ("
            + String.join(", ", stmt.getValueMap().keySet())
            + ") VALUES ("
            + String.join(", ", stmt.getValueMap().values())
            + ")";
    }

    private static String generateSql(DeleteStatement stmt){
        StringBuilder query = new StringBuilder("DELETE FROM ")
                .append(stmt.getTableName())
                .append(" WHERE (");
        appendValueMapToStringBuilder(stmt.getValueMap(), query, ") AND (");
        query.append(")");
        return query.toString();
    }

    private static String generateSql(UpdateStatement stmt){
        StringBuilder query = new StringBuilder("UPDATE ")
                .append(stmt.getTableName())
                .append(" SET ");
        appendValueMapToStringBuilder(stmt.getValueMap(), query, ", ");
        query.append(" WHERE (");
        appendValueMapToStringBuilder(stmt.getOldValueMap(), query, ") AND (");
        query.append(")");
        return query.toString();
    }

}

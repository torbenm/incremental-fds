package org.mp.naumann.database.jdbc.sql;

import org.mp.naumann.database.statement.DefaultDeleteStatement;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;
import org.mp.naumann.database.statement.UpdateStatement;

import java.sql.JDBCType;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlQueryBuilder {

    public static String generateSql(Statement stmt) throws QueryBuilderException {
        if (stmt instanceof InsertStatement) {
            return InsertStatementQueryBuilder.get().generateSingle((InsertStatement) stmt);
        }

        if (stmt instanceof DeleteStatement) {
            return DeleteStatementQueryBuilder.get().generateSingle((DeleteStatement) stmt);
        }

        if (stmt instanceof UpdateStatement) {
            UpdateStatement update = (UpdateStatement) stmt;
            InsertStatement insert = new DefaultInsertStatement(update.getNewValueMap(), update.getSchema(), update.getTableName());
            DeleteStatement delete = new DefaultDeleteStatement(update.getOldValueMap(), update.getSchema(), update.getTableName());
            return generateSql(delete) + "\n" + generateSql(insert);
        }

        throw new QueryBuilderException("Statement has unknown type.");
    }

    public static String generateSql(StatementGroup statements) throws QueryBuilderException {
        StringBuilder sb = new StringBuilder();
        for (Statement stmt : statements.getStatements()) {
            sb.append(generateSql(stmt));
            sb.append("\n");
        }
        return sb.toString();
    }

    static String formatKey(String key) {
        return (key.startsWith(":") ? "\"" + key + "\"" : key);
    }

    private static String formatKeyForValue(String key, String value) {
        return (((value != null) && value.isEmpty()) ? "COALESCE(" + key + ", '')" : key);
    }

    private static String equalsSeparator(String value, boolean isValueClause) {
        return (((value == null) && isValueClause) ? " IS " : " = ");
    }

    static String formatValue(String value, JDBCType jdbcType) {
        if (value == null)
            return "NULL";
        else if (jdbcType == JDBCType.VARCHAR)
            return "'" + value.replace("'", "''") + "'";
        else
            return value;
    }

    static String toKeyEqualsValueMap(Map<String, String> valueMap, Statement stmt, String separator, boolean isValueClause) {
        return valueMap
                .entrySet()
                .parallelStream()
                .map(n -> formatKeyForValue(formatKey(n.getKey()), n.getValue()) + equalsSeparator(n.getValue(), isValueClause) + formatValue(n.getValue(), stmt.getJDBCType(n.getKey())))
                .collect(Collectors.joining(separator));
    }
}

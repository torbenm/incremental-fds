package org.mp.naumann.database.jdbc.sql;

import org.mp.naumann.database.statement.*;

import java.sql.JDBCType;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlQueryBuilder {

    public static String generateSql(Statement stmt) throws QueryBuilderException {
        if (stmt instanceof InsertStatement) {
            return InsertStatementQueryBuilder.get().generateSingle((InsertStatement)stmt);
        }

        if (stmt instanceof DeleteStatement) {
            return DeleteStatementQueryBuilder.get().generateSingle((DeleteStatement) stmt);
        }

        if (stmt instanceof UpdateStatement) {
            return UpdateStatementQueryBuilder.get().generateSingle((UpdateStatement) stmt);
        }

        throw new QueryBuilderException("Statement has unknown type.");
    }

    public static String generateSql(StatementGroup statements) throws QueryBuilderException {
        return Stream.of(
                InsertStatementQueryBuilder.get().generateMulti(statements.getInsertStatements()),
                DeleteStatementQueryBuilder.get().generateMulti(statements.getDeleteStatements()),
                UpdateStatementQueryBuilder.get().generateMulti(statements.getUpdateStatements())
        ).collect(Collectors.joining("\n"));
    }

    static String formatKey(String key) {
        return (key.startsWith(":") ? "\"" + key + "\"" : key);
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

    static String toKeyEqualsValueMap(Map<String, String> valueMap, Statement stmt, String separator, boolean isValueClause){
        return valueMap
                .entrySet()
                .parallelStream()
                .map(n -> formatKey(n.getKey()) + equalsSeparator(n.getValue(), isValueClause) + formatValue(n.getValue(), stmt.getJDBCType(n.getKey())))
                .collect(Collectors.joining(separator));
    }
}

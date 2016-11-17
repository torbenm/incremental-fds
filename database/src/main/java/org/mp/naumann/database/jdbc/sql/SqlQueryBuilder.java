package org.mp.naumann.database.jdbc.sql;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;
import org.mp.naumann.database.statement.UpdateStatement;

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

    static String formatValue(String value) {
        return value.replace("'", "''");
    }

    static String toKeyEqualsValueMap(Map<String, String> valueMap, String seperator){
        return valueMap
                .entrySet()
                .parallelStream()
                .map(n -> n.getKey() + " = '" + formatValue(n.getValue()) + "'")
                .collect(Collectors.joining(seperator));
    }
}

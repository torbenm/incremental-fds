package org.mp.naumann.database.jdbc.sql;

import org.mp.naumann.database.statement.*;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlQueryBuilder {


    public static String generateSql(Statement stmt){
        if (stmt instanceof InsertStatement) {
            return InsertStatementQueryBuilder.get().generateSingle((InsertStatement)stmt);
        }

        if (stmt instanceof DeleteStatement) {
            return DeleteStatementQueryBuilder.get().generateSingle((DeleteStatement) stmt);
        }

        if (stmt instanceof UpdateStatement) {
            return UpdateStatementQueryBuilder.get().generateSingle((UpdateStatement) stmt);
        }

        return "ERROR! However, this case should never occur.";
    }


    public static String generateSql(StatementGroup statements){
        return Stream.of(
                InsertStatementQueryBuilder.get().generateMulti(statements.getInsertStatements()),
                DeleteStatementQueryBuilder.get().generateMulti(statements.getDeleteStatements()),
                UpdateStatementQueryBuilder.get().generateMulti(statements.getUpdateStatements())
        ).collect(Collectors.joining("\n"));
    }



    static String toKeyEqualsValueMap(Map<String, String> valueMap, String seperator){
        return valueMap
                .entrySet()
                .parallelStream()
                .map(n -> n.getKey() + " = '" + n.getValue()+"'")
                .collect(Collectors.joining(seperator));
    }
}

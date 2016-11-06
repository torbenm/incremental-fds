package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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


    static String generateSql(StatementGroup statements){
        return Stream.of(
                generateSqlForInsertStatements(statements.getInsertStatements()),
                generateSqlForDeleteStatements(statements.getDeleteStatements()),
                generateSqlForUpdateStatements(statements.getUpdateStatements())
        ).collect(Collectors.joining("\n"));
    }

    protected static String generateSqlForInsertStatements(List<InsertStatement> stmts){
        if(stmts.size() > 0){
            InsertStatement initial = stmts.get(0);

            List<String> queries = new ArrayList<>();
            List<Map<String, String>> valueSets = new ArrayList<>();

            for(InsertStatement stmt : stmts){
                if(stmt.isOfEqualSchema(initial)){
                    valueSets.add(stmt.getValueMap());
                }else{
                    queries.add(generateSql(stmt));
                }
            }
            queries.add(insertStartStatement(initial) +
                    valueSets.parallelStream()
                            .map(SqlQueryBuilder::insertCreateValueSet)
                            .collect(Collectors.joining(", "))
                    + ";"
            );
            return queries.stream().collect(Collectors.joining("\n"));
        }
        return "";
    }

    private static String generateSql(InsertStatement stmt){
        return insertStartStatement(stmt)
            + insertCreateValueSet(stmt.getValueMap())
            + ";";
    }

    private static String insertStartStatement(InsertStatement stmt){
        return "INSERT INTO "
                + stmt.getTableName()
                + " "
                + insertCreateKeySet(stmt.getValueMap())
                + " VALUES ";
    }

    private static String insertCreateValueSet(Map<String, String> stmtValues){
        String values = stmtValues.values()
                .parallelStream()
                .map(n->"'"+n+"'")
                .collect(Collectors.joining(", "));
        return "("+values+")";
    }

    private static String insertCreateKeySet(Map<String, String> stmtKeys){
        return "("+String.join(", ", stmtKeys.keySet())+")";
    }


    protected static String generateSqlForDeleteStatements(List<DeleteStatement> stmts){
        if(stmts.size() > 0){
            DeleteStatement initial = stmts.get(0);

            List<String> queries = new ArrayList<>();
            List<DeleteStatement> deleteStatements = new ArrayList<>();

            for(DeleteStatement stmt : stmts){
                if(DeleteStatement.areEqualSchema(initial, stmt)){
                    deleteStatements.add(stmt);
                }else{
                    queries.add(generateSql(stmt));
                }
            }
            queries.add(deleteStartStatement(initial) +
                    deleteConcatStatements(deleteStatements)
                    + ";"
            );
            return queries.stream().collect(Collectors.joining("\n"));
        }
        return "";
    }

    private static String generateSql(DeleteStatement stmt){
        return  deleteStartStatement(stmt) +
                (stmt.getValueMap().size() > 0 ?
                        " WHERE (" + toKeyEqualsValueMap(stmt.getValueMap(), " AND ") +")" : "")
                +";";
    }

    private static String deleteStartStatement(DeleteStatement stmt){
        return "DELETE FROM "+
                stmt.getTableName();
    }

    private static String deleteConcatStatements(List<DeleteStatement> stmt){
        return stmt
                .parallelStream()
                .filter(n -> n.getValueMap().size() > 0)
                .map(n -> "("+toKeyEqualsValueMap(n.getValueMap(), " AND ")+")")
                .collect(Collectors.joining(" OR "));
    }


    protected static String generateSqlForUpdateStatements(List<UpdateStatement> stmt){
        return stmt
                .parallelStream()
                .map(SqlQueryBuilder::generateSql)
                .collect(Collectors.joining("\n"));
    }

    private static String generateSql(UpdateStatement stmt){
        StringBuilder query = new StringBuilder("UPDATE ")
                .append(stmt.getTableName())
                .append(" SET ");
        query.append(toKeyEqualsValueMap(stmt.getValueMap(), ", "));
        query.append(" WHERE ");
        query.append(toKeyEqualsValueMap(stmt.getOldValueMap(),  " AND "));
        query.append(";");
        return query.toString();
    }

    private static String toKeyEqualsValueMap(Map<String, String> valueMap, String seperator){
        return valueMap
                .entrySet()
                .parallelStream()
                .map(n -> n.getKey() + " = " + n.getValue())
                .collect(Collectors.joining(seperator));
    }
}

package org.mp.naumann.database.jdbc.sql;

import org.mp.naumann.database.statement.InsertStatement;

import java.util.stream.Collectors;

class InsertStatementQueryBuilder implements StatementQueryBuilder<InsertStatement> {

    private static InsertStatementQueryBuilder instance;

    public static InsertStatementQueryBuilder get(){
        if(instance == null)
            instance = new InsertStatementQueryBuilder();
        return instance;
    }

    InsertStatementQueryBuilder(){ }

    @Override
    public String openStatement(InsertStatement statement){
        return "INSERT INTO " + getCompleteTableName(statement) + " ";
    }

    @Override
    public String buildKeyClause(InsertStatement statement){
        return "(" +
                statement
                    .getValueMap()
                    .keySet()
                    .stream().collect(Collectors.joining(", "))
                + ") VALUES ";
    }

    @Override
    public String buildValueClause(InsertStatement statement){
        return "(" +
                statement
                .getValueMap()
                .entrySet()
                .parallelStream()
                .map(e -> SqlQueryBuilder.formatValue(e.getValue(), statement.getJDBCType(e.getKey())))
                .collect(Collectors.joining(", ")) +
                ")";
    }
}

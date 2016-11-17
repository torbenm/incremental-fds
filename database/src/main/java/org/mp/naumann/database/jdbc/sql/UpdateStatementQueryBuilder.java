package org.mp.naumann.database.jdbc.sql;

import java.util.List;
import java.util.stream.Collectors;

import org.mp.naumann.database.statement.UpdateStatement;

public class UpdateStatementQueryBuilder implements StatementQueryBuilder<UpdateStatement> {

    private static UpdateStatementQueryBuilder instance;

    public static UpdateStatementQueryBuilder get(){
        if(instance == null)
            instance = new UpdateStatementQueryBuilder();
        return instance;
    }

    protected  UpdateStatementQueryBuilder(){}

    @Override
    public String generateSingle(UpdateStatement statement) {
        return openStatement(statement)
                + buildSetStatement(statement)
                + buildWhereClause(statement) + ";";
    }

    @Override
    public String generateMulti(List<UpdateStatement> statements) {
        return statements
                .parallelStream()
                .map(this::generateSingle)
                .collect(Collectors.joining("\n"));
    }

    protected String openStatement(UpdateStatement statement){
        return "UPDATE " + statement.getSchema() + "." + statement.getTableName();
    }

    protected String buildSetStatement(UpdateStatement statement){
        return " SET "+ SqlQueryBuilder.toKeyEqualsValueMap(statement.getValueMap(), ", ");
    }
    protected String buildWhereClause(UpdateStatement statement){
        return " WHERE " + SqlQueryBuilder.toKeyEqualsValueMap(statement.getOldValueMap(),  " AND ");
    }
}

package org.mp.naumann.database.jdbc.sql;

import org.mp.naumann.database.statement.UpdateStatement;

class UpdateStatementQueryBuilder implements StatementQueryBuilder<UpdateStatement> {

    private static UpdateStatementQueryBuilder instance;

    public static UpdateStatementQueryBuilder get(){
        if(instance == null)
            instance = new UpdateStatementQueryBuilder();
        return instance;
    }

    private UpdateStatementQueryBuilder() { }

    @Override
    public void validateStatement(UpdateStatement statement) throws QueryBuilderException {
        StatementQueryBuilder.super.validateStatement(statement);
        if (!(statement.getValueMap().size() == statement.getOldValueMap().size()))
            throw new QueryBuilderException("Value maps for UpdateStatement must have same size");
    }


    @Override
    public String openStatement(UpdateStatement statement){
        return "UPDATE " + getCompleteTableName(statement);
    }

    @Override
    public String buildKeyClause(UpdateStatement statement){
        return " SET "+ SqlQueryBuilder.toKeyEqualsValueMap(statement.getValueMap(), statement, ", ", false);
    }

    @Override
    public String buildValueClause(UpdateStatement statement){
        return " WHERE " + SqlQueryBuilder.toKeyEqualsValueMap(statement.getOldValueMap(), statement, " AND ", true);
    }
}

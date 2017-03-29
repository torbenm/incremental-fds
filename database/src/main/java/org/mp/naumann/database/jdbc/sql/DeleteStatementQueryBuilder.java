package org.mp.naumann.database.jdbc.sql;

import org.mp.naumann.database.statement.DeleteStatement;

class DeleteStatementQueryBuilder implements StatementQueryBuilder<DeleteStatement> {

    private static DeleteStatementQueryBuilder instance;

    private DeleteStatementQueryBuilder() {
    }

    public static DeleteStatementQueryBuilder get() {
        if (instance == null)
            instance = new DeleteStatementQueryBuilder();
        return instance;
    }

    @Override
    public String getMultiConnector() {
        return " OR ";
    }

    @Override
    public String openStatement(DeleteStatement statement) {
        return "DELETE FROM " + getCompleteTableName(statement);
    }

    @Override
    public String buildKeyClause(DeleteStatement statement) {
        return " WHERE ";
    }

    @Override
    public String buildValueClause(DeleteStatement statement) {
        return "(" + SqlQueryBuilder.toKeyEqualsValueMap(statement.getValueMap(), statement, " AND ", true) + ")";
    }

}

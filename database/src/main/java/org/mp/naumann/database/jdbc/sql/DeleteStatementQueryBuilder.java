package org.mp.naumann.database.jdbc.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.mp.naumann.database.statement.DeleteStatement;

class DeleteStatementQueryBuilder implements StatementQueryBuilder<DeleteStatement> {

    private static DeleteStatementQueryBuilder instance;

    public static DeleteStatementQueryBuilder get(){
        if(instance == null)
            instance = new DeleteStatementQueryBuilder();
        return instance;
    }

    private DeleteStatementQueryBuilder() { }

    @Override
    public String generateSingle(DeleteStatement statement) {
        return openStatement(statement)
                + createWhereClause(statement)
                + ";";
    }

    @Override
    public String generateMulti(List<DeleteStatement> statements) {
        if (statements.size() > 0) {
            DeleteStatement base = statements.get(0);

            List<DeleteStatement> equal = new ArrayList<>();
            List<DeleteStatement> unequal = new ArrayList<>();

            for (DeleteStatement stmt : statements) {
                if (base.isOfEqualSchema(stmt)) {
                    equal.add(stmt);
                } else {
                    unequal.add(stmt);
                }
            }

            String query = unequal.size() > 0 ?
                    unequal
                            .parallelStream()
                            .map(this::generateSingle)
                            .collect(Collectors.joining("\n")) + "\n" : "";
            query += equal.size() > 0 ?
                    openStatement(base) +
                            createWhereClause(equal) + ";" : "";
            return query;
        }
        return "";
    }

    private String openStatement(DeleteStatement statement) {
        return "DELETE FROM " + statement.getSchema() + "." + statement.getTableName();
    }

    protected String generateKeyValueMap(DeleteStatement statement) {
        return SqlQueryBuilder.toKeyEqualsValueMap(statement.getValueMap(), " AND ");
    }

    protected String createWhereClause(DeleteStatement statement) {
        if (statement.getValueMap().size() > 0) {
            return " WHERE " + generateKeyValueMap(statement);
        }
        return "";
    }

    protected String createWhereClause(List<DeleteStatement> statements) {
        return " WHERE " + statements
                .parallelStream()
                .filter(n -> n.getValueMap().size() > 0)
                .map(n -> "(" + SqlQueryBuilder.toKeyEqualsValueMap(n.getValueMap(), " AND ") + ")")
                .collect(Collectors.joining(" OR "));
    }
}

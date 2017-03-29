package org.mp.naumann.database.jdbc.sql;

import org.mp.naumann.database.statement.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

interface StatementQueryBuilder<T extends Statement> {

    default void validateStatement(T statement) throws QueryBuilderException {
        if (statement.isEmpty())
            throw new QueryBuilderException("Statement must not have an empty value list.");
    }

    default String formatName(String name) {
        return name.contains(" ") ? "\"" + name + "\"" : name;
    }

    default String getCompleteTableName(T statement) {
        String schema = (statement.getSchema().isEmpty() ? "" : formatName(statement.getSchema()) + ".");
        return schema + formatName(statement.getTableName());
    }

    default String generateSingle(T statement) throws QueryBuilderException {
        validateStatement(statement);
        return openStatement(statement) + buildKeyClause(statement) + buildValueClause(statement) + ";";
    }

    default String generateMulti(List<T> statements) throws QueryBuilderException {
        if (statements.size() <= 0)
            return "";

        for (T statement : statements) validateStatement(statement);

        List<List<T>> layoutGroups = new ArrayList<>();

        for (T stmt : statements) {
            boolean found = false;
            for (List<T> layoutGroup : layoutGroups) {
                if (layoutGroup.get(0).isOfEqualLayout(stmt)) {
                    found = true;
                    layoutGroup.add(stmt);
                }
            }
            if (!found) {
                layoutGroups.add(new ArrayList<>());
                layoutGroups.get(layoutGroups.size() - 1).add(stmt);
            }
        }

        StringBuilder queryBuilder = new StringBuilder();
        for (List<T> layoutGroup : layoutGroups) {
            queryBuilder.append(openStatement(layoutGroup.get(0)));
            queryBuilder.append(buildKeyClause(layoutGroup.get(0)));
            queryBuilder.append(layoutGroup
                    .parallelStream()
                    .map(this::buildValueClause)
                    .collect(Collectors.joining(getMultiConnector())));
            queryBuilder.append(";\n");
        }
        return queryBuilder.toString().trim();
    }

    default String getMultiConnector() {
        return ", ";
    }

    String openStatement(T statement);

    String buildKeyClause(T statement);

    String buildValueClause(T statement);

}

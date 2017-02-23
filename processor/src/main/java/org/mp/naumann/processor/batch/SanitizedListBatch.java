package org.mp.naumann.processor.batch;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SanitizedListBatch extends ListBatch {

    public SanitizedListBatch(List<Statement> statements, String schema, String tableName) {
        super(statements, schema, tableName);
        sanitizeStatements(statements);
    }

    private void sanitizeStatements(List<Statement> statements) {
        Map<Map<String, String>, List<Statement>> clusters = new HashMap<>();
        List<Statement> statementsToRemove = new ArrayList<>();

        for (Statement statement: statements) {
            if (statement instanceof InsertStatement) {
                // create a new bucket for this record
                Map<String, String> valueMap = ((InsertStatement) statement).getValueMap();
                clusters.put(valueMap, new ArrayList<>());
                clusters.get(valueMap).add(statement);
            } else if (statement instanceof DeleteStatement) {
                // if we have a bucket for this record, mark all statements in it & this one as toDelete
                Map<String, String> valueMap = ((DeleteStatement) statement).getValueMap();
                if (clusters.containsKey(valueMap)) {
                    statementsToRemove.addAll(clusters.get(valueMap));
                    statementsToRemove.add(statement);
                    clusters.remove(valueMap);
                }
            } else if (statement instanceof UpdateStatement) {
                // create a new bucket, and if there was a bucket for the old values, merge it into the new one
                Map<String, String> valueMap = ((UpdateStatement) statement).getValueMap();
                Map<String, String> oldValueMap = ((UpdateStatement) statement).getOldValueMap();
                clusters.put(valueMap, new ArrayList<>());
                clusters.get(valueMap).add(statement);
                if (clusters.containsKey(oldValueMap)) {
                    clusters.get(valueMap).addAll(0, clusters.get(oldValueMap));
                    clusters.remove(oldValueMap);
                }
            }
        }

        statements.removeAll(statementsToRemove);
    }
}

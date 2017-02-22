package org.mp.naumann.algorithms.fd.incremental.datastructures;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.StatementVisitor;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractStatementApplier implements StatementVisitor {
    private final Set<Integer> inserted = new HashSet<>();
    private final Set<Integer> insertedUpdate = new HashSet<>();
    private final Set<Integer> deleted = new HashSet<>();
    private final Set<Integer> deletedUpdate = new HashSet<>();

    @Override
    public void visit(DeleteStatement delete) {
        Collection<Integer> removed = removeRecord(delete.getValueMap());
        deleted.addAll(removed);
    }

    @Override
    public void visit(UpdateStatement update) {
        Collection<Integer> removed = removeRecord(update.getOldValueMap());
        deletedUpdate.addAll(removed);
        int inserted = addRecord(update.getNewValueMap());
        insertedUpdate.add(inserted);
    }

    @Override
    public void visit(InsertStatement insert) {
        int inserted = addRecord(insert.getValueMap());
        insertedUpdate.add(inserted);
    }

    public Set<Integer> getInserted() {
        return inserted;
    }

    public Set<Integer> getInsertedUpdate() {
        return insertedUpdate;
    }

    public Set<Integer> getDeleted() {
        return deleted;
    }

    public Set<Integer> getDeletedUpdate() {
        return deletedUpdate;
    }

    protected abstract int addRecord(Map<String, String> valueMap);

    protected abstract Collection<Integer> removeRecord(Map<String, String> valueMap);
}

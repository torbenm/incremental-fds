package org.mp.naumann.algorithms.fd.incremental.datastructures;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.StatementVisitor;
import org.mp.naumann.database.statement.UpdateStatement;

public abstract class AbstractStatementApplier implements StatementVisitor {

	private final Set<Integer> inserted = new HashSet<>();
	private final Set<Integer> deleted = new HashSet<>();

	@Override
	public void visit(DeleteStatement delete) {
		Benchmark benchmark = Benchmark.start("Delete", Benchmark.DEFAULT_LEVEL + 7);
		delete(delete.getValueMap());
		benchmark.finish();
	}

	@Override
	public void visit(UpdateStatement update) {
		Benchmark benchmark = Benchmark.start("Update", Benchmark.DEFAULT_LEVEL + 7);
		Collection<Integer> removed = delete(update.getOldValueMap());
		removed.forEach(id -> insert(update.getNewValueMap()));
		benchmark.finish();
	}

	private Collection<Integer> delete(Map<String, String> oldValueMap) {
		Collection<Integer> removed = removeRecord(oldValueMap);
		deleted.addAll(removed);
		return removed;
	}

	private void insert(Map<String, String> newValueMap) {
		int insertedRecord = addRecord(newValueMap);
		inserted.add(insertedRecord);
	}

	@Override
	public void visit(InsertStatement insert) {
		Benchmark benchmark = Benchmark.start("Insert", Benchmark.DEFAULT_LEVEL + 7);
		insert(insert.getValueMap());
		benchmark.finish();
	}

	public Set<Integer> getInserted() {
		return inserted;
	}

	public Set<Integer> getDeleted() {
		return deleted;
	}

	protected abstract int addRecord(Map<String, String> valueMap);

	protected abstract Collection<Integer> removeRecord(Map<String, String> valueMap);
}

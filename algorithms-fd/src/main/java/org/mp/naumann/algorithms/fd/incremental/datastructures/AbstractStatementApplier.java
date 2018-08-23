package org.mp.naumann.algorithms.fd.incremental.datastructures;

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.IntConsumer;
import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.StatementVisitor;
import org.mp.naumann.database.statement.UpdateStatement;

public abstract class AbstractStatementApplier implements StatementVisitor {

	private final IntSet inserted = new IntOpenHashSet();
	private final IntSet deleted = new IntOpenHashSet();

	@Override
	public void visit(DeleteStatement delete) {
		Benchmark benchmark = Benchmark.start("Delete", Benchmark.DEFAULT_LEVEL + 7);
		delete(delete.getValueMap());
		benchmark.finish();
	}

	@Override
	public void visit(UpdateStatement update) {
		Benchmark benchmark = Benchmark.start("Update", Benchmark.DEFAULT_LEVEL + 7);
		IntCollection removed = delete(update.getOldValueMap());
		removed.forEach((IntConsumer) id -> insert(update.getNewValueMap()));
		benchmark.finish();
	}

	private IntCollection delete(Map<String, String> oldValueMap) {
		IntCollection removed = removeRecord(oldValueMap);
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

	public IntSet getInserted() {
		return inserted;
	}

	public IntSet getDeleted() {
		return deleted;
	}

	protected abstract int addRecord(Map<String, String> valueMap);

	protected abstract IntCollection removeRecord(Map<String, String> valueMap);
}

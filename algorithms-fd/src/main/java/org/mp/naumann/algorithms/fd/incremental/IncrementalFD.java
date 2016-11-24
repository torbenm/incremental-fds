package org.mp.naumann.algorithms.fd.incremental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.fd.FDIntermediateDatastructure;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

public class IncrementalFD implements IncrementalAlgorithm<List<FunctionalDependency>, FDIntermediateDatastructure> {

	private final List<String> columns;
	private List<Set<String>> columnValues;
	private FDTree posCover;
	private final String tableName;
	private List<PositionListIndex> plis;
	private final List<ResultListener<List<FunctionalDependency>>> resultListeners = new ArrayList<>();
	private int[][] compressedRecords;
	private MemoryGuardian memoryGuardian = new MemoryGuardian(true);

	public IncrementalFD(List<String> columns, String tableName) {
		this.columns = columns;
		this.tableName = tableName;
	}

	@Override
	public Collection<ResultListener<List<FunctionalDependency>>> getResultListeners() {
		return resultListeners;
	}

	@Override
	public void addResultListener(ResultListener<List<FunctionalDependency>> listener) {
		this.resultListeners.add(listener);
	}

	@Override
	public List<FunctionalDependency> execute(Batch batch) {
		List<InsertStatement> inserts = batch.getInsertStatements();
		int numAttributes = columns.size();
		List<ObjectOpenHashSet<OpenBitSet>> existingCombinations = new ArrayList<>();
		for (int i = 0; i < numAttributes; i++) {
			existingCombinations.add(new ObjectOpenHashSet<>());
		}
		for (InsertStatement insert : inserts) {
			OpenBitSet existingCombination = new OpenBitSet(numAttributes);
			int i = 0;
			Map<String, String> valueMap = insert.getValueMap();
			for (String column : columns) {
				String value = valueMap.get(column);
				Set<String> set = columnValues.get(i);
				if (set.contains(value)) {
					existingCombination.fastSet(i);
				}
				i++;
			}
			existingCombinations.get((int) existingCombination.cardinality()).add(existingCombination);
		}
		for (InsertStatement insert : inserts) {
			updateDataStructures(insert.getValueMap());
		}
		float efficiencyThreshold = 0.01f;
		boolean validateParallel = true;
		Validator validator = new Validator(posCover, compressedRecords, plis, efficiencyThreshold, validateParallel, memoryGuardian);
		for (int level = 0; level < posCover.getDepth(); level++) {
			List<FDTreeElementLhsPair> toValidate = new ArrayList<>();
			for (FDTreeElementLhsPair fd : posCover.getLevel(level)) {
				for (long i = level; i < existingCombinations.size(); i++) {
					for (OpenBitSet ex : existingCombinations.get((int) i)) {
						if (isLhscontained(fd, ex)) {
							toValidate.add(fd);
							continue;
						}
					}
				}
			}
			try {
				validator.validate(level, toValidate);
			} catch (AlgorithmExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		List<FunctionalDependency> fds = new ArrayList<>();
		posCover.addFunctionalDependenciesInto(fds::add, this.buildColumnIdentifiers(), plis);
		return fds;
	}

	private void updateDataStructures(Map<String, String> valueMap) {
		int i = 0;
		for (String column : columns) {
			String value = valueMap.get(column);
			Set<String> set = columnValues.get(i);
			set.add(value);
			i++;
		}
		// TODO update plis and compressedRecords
		
	}

	private boolean isLhscontained(FDTreeElementLhsPair fd, OpenBitSet ex) {
		return OpenBitSet.andNotCount(fd.getLhs(), ex) == 0;
	}

	@Override
	public void setIntermediateDataStructure(FDIntermediateDatastructure intermediateDataStructure) {
		this.posCover = intermediateDataStructure.getPosCover();
		this.columnValues = intermediateDataStructure.getColumnValues();
		this.plis = intermediateDataStructure.getPlis();
		this.compressedRecords = intermediateDataStructure.getCompressedRecords();
	}

	private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.columns.size());
		for (String attributeName : this.columns)
			columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
		return columnIdentifiers;
	}

}

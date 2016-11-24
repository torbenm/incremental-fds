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
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

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
		CardinalitySet existingCombinations = new CardinalitySet(numAttributes, numAttributes);
		for (InsertStatement insert : inserts) {
			OpenBitSet existingCombination = new OpenBitSet(numAttributes);
			Map<String, String> valueMap = insert.getValueMap();
			int i = 0;
			for (PositionListIndex pli : plis) {
				int columnId = pli.getAttribute();
				String value = valueMap.get(columns.get(columnId));
				Set<String> set = columnValues.get(columnId);
				if (set.contains(value)) {
					existingCombination.fastSet(i);
				}
				i++;
			}
			existingCombinations.add(existingCombination);
		}
		for (InsertStatement insert : inserts) {
			updateDataStructures(insert.getValueMap());
		}
		boolean validateParallel = true;
		Validator validator = new Validator(posCover, compressedRecords, plis, validateParallel, memoryGuardian);
		for (int level = 0; level <= posCover.getDepth(); level++) {
			List<FDTreeElementLhsPair> currentLevel = getFdLevel(level);
			List<FDTreeElementLhsPair> toValidate = new ArrayList<>();
			for (FDTreeElementLhsPair fd : currentLevel) {
				if(canBeViolated(existingCombinations, fd)) {
					toValidate.add(fd);
				}
			}
			System.out.println("Will validate: ");
			toValidate.stream().map(this::toFds).flatMap(Collection::stream).forEach(System.out::println);
//			try {
//				validator.validate(level, toValidate);
//			} catch (AlgorithmExecutionException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		List<FunctionalDependency> fds = new ArrayList<>();
		posCover.addFunctionalDependenciesInto(fds::add, this.buildColumnIdentifiers(), plis);
		return fds;
	}
	
	private List<FunctionalDependency> toFds(FDTreeElementLhsPair fd) {
		OpenBitSet lhs = fd.getLhs();
		OpenBitSet rhsFds = fd.getElement().getFds();
		List<FunctionalDependency> fds = new ArrayList<>();
		for (int rhs = rhsFds.nextSetBit(0); rhs >= 0; rhs = rhsFds.nextSetBit(rhs + 1)) {
            FunctionalDependency fdResult = findFunctionDependency(lhs, rhs, buildColumnIdentifiers(), plis);
            fds.add(fdResult);
        }
		return fds;
	}

    private FunctionalDependency findFunctionDependency(OpenBitSet lhs, int rhs,
                                                        ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis){
        ColumnIdentifier[] columns = new ColumnIdentifier[(int) lhs.cardinality()];
        int j = 0;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            int columnId = plis.get(i).getAttribute(); // Here we translate the column i back to the real column i before the sorting
            columns[j++] = columnIdentifiers.get(columnId);
        }

        ColumnCombination colCombination = new ColumnCombination(columns);
        int rhsId = plis.get(rhs).getAttribute(); // Here we translate the column rhs back to the real column rhs before the sorting
        return new FunctionalDependency(colCombination, columnIdentifiers.get(rhsId));
    }

	public List<FDTreeElementLhsPair> getFdLevel(int level) {
		final List<FDTreeElementLhsPair> currentLevel;
		if (level == 0) {
			currentLevel = new ArrayList<>();
			currentLevel.add(new FDTreeElementLhsPair(this.posCover, new OpenBitSet(columns.size())));
		} else {
			currentLevel = this.posCover.getLevel(level);
		}
		return currentLevel;
	}

	public boolean canBeViolated(CardinalitySet existingCombinations, FDTreeElementLhsPair fd) {
		for (int i = (int) fd.getLhs().cardinality(); i <= existingCombinations.getDepth(); i++) {
			for (OpenBitSet ex : existingCombinations.getLevel(i)) {
				if (BitSetUtils.isContained(fd.getLhs(), ex)) {
					return true;
				}
			}
		}
		return false;
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

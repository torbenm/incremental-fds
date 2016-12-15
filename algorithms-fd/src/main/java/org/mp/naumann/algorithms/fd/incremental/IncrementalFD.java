package org.mp.naumann.algorithms.fd.incremental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.logging.Level;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDIntermediateDatastructure;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.ValueCombination;
import org.mp.naumann.algorithms.fd.structures.ValueCombination.ColumnValue;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import com.google.common.hash.BloomFilter;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class IncrementalFD implements IncrementalAlgorithm<IncrementalFDResult, FDIntermediateDatastructure> {


	private final IncrementalFDVersion VERSION;
    private final boolean VALIDATE_PARALLEL = true;

	private final List<String> columns;
	private FDTree posCover;
	private final String tableName;
	private final List<ResultListener<IncrementalFDResult>> resultListeners = new ArrayList<>();
	private MemoryGuardian memoryGuardian = new MemoryGuardian(true);
	private FDIntermediateDatastructure intermediateDatastructure;
	private boolean initialized = false;

	private IncrementalPLIBuilder incrementalPLIBuilder;
	private BloomFilter<Set<ColumnValue>> filter;
	private final Map<String, Integer> columnsToId = new HashMap<>();

    public IncrementalFD(List<String> columns, String tableName, IncrementalFDVersion version){
        this.columns = columns;
        this.tableName = tableName;
        this.VERSION = version;
    }

	public IncrementalFD(List<String> columns, String tableName) {
		this(columns, tableName, IncrementalFDVersion.LATEST);
	}

	@Override
	public Collection<ResultListener<IncrementalFDResult>> getResultListeners() {
		return resultListeners;
	}

	@Override
	public void addResultListener(ResultListener<IncrementalFDResult> listener) {
		this.resultListeners.add(listener);
	}

	public void initialize() {
		this.posCover = intermediateDatastructure.getPosCover();
		this.filter = intermediateDatastructure.getFilter();
		incrementalPLIBuilder = new IncrementalPLIBuilder(this.VERSION, intermediateDatastructure.getNumRecords(),
				intermediateDatastructure.getClusterMaps(), columns, intermediateDatastructure.getPliSequence(),
				filter);
		int i = 0;
		for (PositionListIndex pli : incrementalPLIBuilder.getPlis()) {
			columnsToId.put(columns.get(pli.getAttribute()), i++);
		}
	}

	@Override
	public IncrementalFDResult execute(Batch batch) {
		if (!initialized) {
			initialize();
			initialized = true;
		}
		SpeedBenchmark.begin(BenchmarkLevel.METHOD_HIGH_LEVEL);
		CardinalitySet existingCombinations = null;
		if (VERSION.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.BLOOM) {
			existingCombinations = getExistingCombinationsWithBloom(batch);
		}
		CompressedDiff diff = incrementalPLIBuilder.update(batch);
		List<PositionListIndex> plis = incrementalPLIBuilder.getPlis();
		int[][] compressedRecords = incrementalPLIBuilder.getCompressedRecord();
		if (VERSION.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.SIMPLE) {
			existingCombinations = getExistingCombinationsSimple(diff);
		}
		Validator validator = new Validator(posCover, compressedRecords, plis, VALIDATE_PARALLEL, memoryGuardian);

		int pruned = 0;
		int validations = 0;
		for (int level = 0; level <= posCover.getDepth(); level++) {
			List<FDTreeElementLhsPair> currentLevel = getFdLevel(level);
			List<FDTreeElementLhsPair> toValidate = new ArrayList<>();
			for (FDTreeElementLhsPair fd : currentLevel) {
				if (existingCombinations == null || canBeViolated(existingCombinations, fd)) {
					toValidate.add(fd);
				} else {
					pruned++;
				}
			}
			FDLogger.log(Level.FINER, "Will validate: ");
			toValidate.stream().map(this::toFds).flatMap(Collection::stream)
					.forEach(v -> FDLogger.log(Level.FINER, v.toString()));
			validations += toValidate.size();
			try {
				validator.validate(level, currentLevel);
			} catch (AlgorithmExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		validator.shutdown();
		FDLogger.log(Level.FINE, "Pruned " + pruned + " validations");
		FDLogger.log(Level.FINE, "Made " + validations + " validations");
		List<FunctionalDependency> fds = new ArrayList<>();
		posCover.addFunctionalDependenciesInto(fds::add, this.buildColumnIdentifiers(), plis);
		SpeedBenchmark.end(BenchmarkLevel.METHOD_HIGH_LEVEL, "Processed one batch, inner measuring");
		return new IncrementalFDResult(fds, validations, pruned);
	}

	public CardinalitySet getExistingCombinationsWithBloom(Batch batch) {
		CardinalitySet existingCombinations;
		existingCombinations = new CardinalitySet(2);
		List<InsertStatement> inserts = batch.getInsertStatements();
		Set<Set<ColumnValue>> innerDoubleCombinations = innerCombinationsToCheck(batch);
		for (InsertStatement insert : inserts) {
			ValueCombination vc = new ValueCombination();
			for (Entry<String, String> entry : insert.getValueMap().entrySet()) {
				vc.add(entry.getKey(), entry.getValue());
			}
			for (Set<ColumnValue> combination : vc.getPowerSet(2)) {
				if (innerDoubleCombinations.contains(combination) || filter.mightContain(combination)) {
					OpenBitSet existing = new OpenBitSet(columns.size());
					for (ColumnValue value : combination) {
						existing.fastSet(columnsToId.get(value.getColumn()));
					}
					existingCombinations.add(existing);
				}
			}
		}
		return existingCombinations;
	}

	public Set<Set<ColumnValue>> innerCombinationsToCheck(Batch batch) {
		List<InsertStatement> inserts = batch.getInsertStatements();
		Map<Set<ColumnValue>, Integer> innerCombinations = new HashMap<>();
		for (InsertStatement insert : inserts) {
			ValueCombination vc = new ValueCombination();
			for (Entry<String, String> entry : insert.getValueMap().entrySet()) {
				vc.add(entry.getKey(), entry.getValue());
			}
			for (Set<ColumnValue> combination : vc.getPowerSet(2)) {
				innerCombinations.merge(combination, 1, Integer::sum);
			}
		}
		Set<Set<ColumnValue>> innerDoubleCombinations = innerCombinations.entrySet().stream()
				.filter(e -> e.getValue() > 1).map(Entry::getKey).collect(Collectors.toSet());
		return innerDoubleCombinations;
	}

	public CardinalitySet getExistingCombinationsSimple(CompressedDiff diff) {
		int numAttributes = columns.size();
		CardinalitySet existingCombinations = new CardinalitySet(numAttributes);
		for (int[] insert : diff.getInsertedRecords()) {
			OpenBitSet existingCombination = findExistingCombinations(insert);
			existingCombinations.add(existingCombination);
		}
		return existingCombinations;
	}

	public OpenBitSet findExistingCombinations(int[] compressedRecord) {
		OpenBitSet existingCombination = new OpenBitSet(compressedRecord.length);
		int i = 0;
		for (int clusterId : compressedRecord) {
			if (clusterId > -1) {
				existingCombination.fastSet(i);
			}
			i++;
		}
		return existingCombination;
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
		for (int i = existingCombinations.getDepth(); i >= (int) fd.getLhs().cardinality(); i--) {
			for (OpenBitSet ex : existingCombinations.getLevel(i)) {
				if (BitSetUtils.isContained(fd.getLhs(), ex)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public void setIntermediateDataStructure(FDIntermediateDatastructure intermediateDataStructure) {
		this.intermediateDatastructure = intermediateDataStructure;
	}

	private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.columns.size());
		for (String attributeName : this.columns)
			columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
		return columnIdentifiers;
	}

	private List<FunctionalDependency> toFds(FDTreeElementLhsPair fd) {
		OpenBitSet lhs = fd.getLhs();
		OpenBitSet rhsFds = fd.getElement().getFds();
		List<FunctionalDependency> fds = new ArrayList<>();
		for (int rhs = rhsFds.nextSetBit(0); rhs >= 0; rhs = rhsFds.nextSetBit(rhs + 1)) {
			FunctionalDependency fdResult = findFunctionDependency(lhs, rhs, buildColumnIdentifiers(),
					incrementalPLIBuilder.getPlis());
			fds.add(fdResult);
		}
		return fds;
	}

	private FunctionalDependency findFunctionDependency(OpenBitSet lhs, int rhs,
			ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) {
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

}

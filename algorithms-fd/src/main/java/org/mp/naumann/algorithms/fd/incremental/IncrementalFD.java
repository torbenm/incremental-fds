package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDIntermediateDatastructure;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.incremental.bloom.AdvancedBloomPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.bloom.BloomPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.bloom.SimpleBloomPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.simple.SimplePruningStrategy;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

public class IncrementalFD implements IncrementalAlgorithm<IncrementalFDResult, FDIntermediateDatastructure> {


    private final IncrementalFDVersion version;
    private static final boolean VALIDATE_PARALLEL = true;

    private final List<String> columns;
    private FDTree posCover;
    private final String tableName;
    private final List<ResultListener<IncrementalFDResult>> resultListeners = new ArrayList<>();
    private MemoryGuardian memoryGuardian = new MemoryGuardian(true);
    private FDIntermediateDatastructure intermediateDatastructure;
    private boolean initialized = false;

    private IncrementalPLIBuilder incrementalPLIBuilder;
    private BloomPruningStrategy advancedBloomPruning;
    private SimplePruningStrategy simplePruning;
    private BloomPruningStrategy bloomPruning;

    public IncrementalFD(List<String> columns, String tableName, IncrementalFDVersion version) {
        this.columns = columns;
        this.tableName = tableName;
        this.version = version;
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

    @Override
    public void initialize() {
        this.posCover = intermediateDatastructure.getPosCover();
        int numRecords = intermediateDatastructure.getNumRecords();
        List<Integer> pliSequence = intermediateDatastructure.getPliSequence();
        List<HashMap<String, IntArrayList>> clusterMaps = intermediateDatastructure.getClusterMaps();
        if (version.getPruningStrategy() == IncrementalFDVersion.PruningStrategy.BLOOM) {
            bloomPruning = new SimpleBloomPruningStrategy(columns, numRecords, pliSequence, clusterMaps);
            bloomPruning.initialize();
        }
        if (version.getPruningStrategy() == IncrementalFDVersion.PruningStrategy.BLOOM_ADVANCED) {
            advancedBloomPruning = new AdvancedBloomPruningStrategy(columns, numRecords, pliSequence, clusterMaps, posCover);
            advancedBloomPruning.initialize();
        }
        if (version.getPruningStrategy() == IncrementalFDVersion.PruningStrategy.SIMPLE) {
            simplePruning = new SimplePruningStrategy(columns);
        }
        incrementalPLIBuilder = new IncrementalPLIBuilder(this.version, numRecords,
                clusterMaps, columns, pliSequence);
    }

    @Override
    public IncrementalFDResult execute(Batch batch) {
        if (!initialized) {
            FDLogger.log(Level.FINE, "Initializing IncrementalFD");
            initialize();
            initialized = true;
        }
        FDLogger.log(Level.FINE, "Started IncrementalFD for new Batch");
        SpeedBenchmark.begin(BenchmarkLevel.METHOD_HIGH_LEVEL);
        CardinalitySet existingCombinations = null;
        if (version.getPruningStrategy() == IncrementalFDVersion.PruningStrategy.BLOOM) {
            existingCombinations = bloomPruning.getExistingCombinations(batch);
        }
        if (version.getPruningStrategy() == IncrementalFDVersion.PruningStrategy.BLOOM_ADVANCED) {
            existingCombinations = advancedBloomPruning.getExistingCombinations(batch);
        }
        CompressedDiff diff = incrementalPLIBuilder.update(batch);
        List<PositionListIndex> plis = incrementalPLIBuilder.getPlis();
        int[][] compressedRecords = incrementalPLIBuilder.getCompressedRecord();
        if (version.getPruningStrategy() == IncrementalFDVersion.PruningStrategy.SIMPLE) {
            existingCombinations = simplePruning.getExistingCombinations(diff);
        }

        FDLogger.log(Level.FINE, "Finished collecting existing combinations");
        Validator validator = new Validator(posCover, compressedRecords, plis, VALIDATE_PARALLEL, memoryGuardian);

        int pruned = 0;
        int validations = 0;
        for (int level = 0; level <= posCover.getDepth(); level++) {
            List<FDTreeElementLhsPair> currentLevel = FDTreeUtils.getFdLevel(posCover, level);
            List<FDTreeElementLhsPair> toValidate = new ArrayList<>();
            for (FDTreeElementLhsPair fd : currentLevel) {
                if (existingCombinations == null || canBeViolated(existingCombinations, fd)) {
                    toValidate.add(fd);
                } else {
                    pruned++;
                }
            }
            FDLogger.log(Level.FINEST, "Will validate: ");
            toValidate.stream().map(this::toFds).flatMap(Collection::stream)
                    .forEach(v -> FDLogger.log(Level.FINEST, v.toString()));
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

    private boolean canBeViolated(CardinalitySet existingCombinations, FDTreeElementLhsPair fd) {
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

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
import org.mp.naumann.algorithms.fd.hyfd.FDList;
import org.mp.naumann.algorithms.fd.incremental.bloom.CurrentFDBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.bloom.BloomPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.bloom.AllCombinationsBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.simple.ExistingValuesPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolationCollection;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class IncrementalFD implements IncrementalAlgorithm<IncrementalFDResult, FDIntermediateDatastructure> {

    private static final boolean VALIDATE_PARALLEL = true;
    private static final float EFFICIENCY_THRESHOLD = 0.01f;
    private IncrementalFDVersion version = IncrementalFDVersion.LATEST;

    private final List<String> columns;
    private FDTree posCover;
    private final String tableName;
    private final List<ResultListener<IncrementalFDResult>> resultListeners = new ArrayList<>();
    private MemoryGuardian memoryGuardian = new MemoryGuardian(true);
    private FDIntermediateDatastructure intermediateDatastructure;
    private boolean initialized = false;

    private IncrementalPLIBuilder incrementalPLIBuilder;
    private BloomPruningStrategy advancedBloomPruning;
    private ExistingValuesPruningStrategy simplePruning;
    private BloomPruningStrategy bloomPruning;
    private FDSet negCover;
    private ViolationCollection violationCollection;

    public IncrementalFD(List<String> columns, String tableName, IncrementalFDVersion version) {
        this(columns, tableName);
        this.version = version;
    }


    public IncrementalFD(List<String> columns, String tableName) {
        this.columns = columns;
        this.tableName = tableName;
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
        this.negCover = intermediateDatastructure.getNegCover();

        PLIBuilder pliBuilder = intermediateDatastructure.getPliBuilder();
        List<Integer> pliSequence = pliBuilder.getPliOrder();

        List<String> orderedColumns = pliSequence.stream().map(columns::get).collect(Collectors.toList());

        List<HashMap<String, IntArrayList>> clusterMaps = intermediateDatastructure.getPliBuilder().getClusterMaps();

        if (version.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.BLOOM) {
            bloomPruning = new BloomPruningStrategy(orderedColumns).addGenerator(new AllCombinationsBloomGenerator(2));
            bloomPruning.initialize(clusterMaps, pliBuilder.getNumLastRecords(), pliSequence);
        }
        if (version.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.BLOOM_ADVANCED) {
            advancedBloomPruning = new BloomPruningStrategy(orderedColumns)
                    .addGenerator(new CurrentFDBloomGenerator(posCover));
            advancedBloomPruning.initialize(clusterMaps, pliBuilder.getNumLastRecords(), pliSequence);
        }
        if (version.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.SIMPLE) {
            simplePruning = new ExistingValuesPruningStrategy(columns);
        }
        if(version.getDeletePruningStrategy() == IncrementalFDVersion.DeletePruningStrategy.ANNOTATION){
            violationCollection = intermediateDatastructure.getViolatingValues();
        }

        incrementalPLIBuilder = new IncrementalPLIBuilder(pliBuilder, this.version, this.columns);
    }

    @Override
    public IncrementalFDResult execute(Batch batch) throws AlgorithmExecutionException {
        if (!initialized) {
            FDLogger.log(Level.FINE, "Initializing IncrementalFD");
            initialize();
            FDLogger.log(Level.INFO, intermediateDatastructure.getViolatingValues().toString());
            initialized = true;
        }

        FDLogger.log(Level.FINE, "Started IncrementalFD for new Batch");
        SpeedBenchmark.begin(BenchmarkLevel.METHOD_HIGH_LEVEL);

        // Update datastructures with current batch
        CompressedDiff diff = incrementalPLIBuilder.update(batch);
        List<PositionListIndex> plis = incrementalPLIBuilder.getPlis();
        int[][] compressedRecords = incrementalPLIBuilder.getCompressedRecord();

        if(diff.getDeletedRecords().length > 0){
            return pruneDeleteBatch(diff, plis, compressedRecords);
        }

        // Create Validator, Sampler & Inductor
        IncrementalValidator validator = new IncrementalValidator(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD, VALIDATE_PARALLEL, memoryGuardian);
        Sampler sampler = new Sampler(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD,
                intermediateDatastructure.getValueComparator(), this.memoryGuardian);
        Inductor inductor = new Inductor(negCover, posCover, this.memoryGuardian);

        // Add validation pruner for inserts
        if (version.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.BLOOM) {
            validator.addValidationPruner(bloomPruning.analyzeBatch(batch));
        }
        if (version.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.BLOOM_ADVANCED) {
            validator.addValidationPruner(advancedBloomPruning.analyzeBatch(batch));
        }
        if (version.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.SIMPLE) {
            validator.addValidationPruner(simplePruning.analyzeDiff(diff));
        }
        FDLogger.log(Level.FINE, "Finished building pruning strategies");

        // Actual algorithm execution
        List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        int i = 1;
        do {
            FDLogger.log(Level.FINE, "Started round " + i);
            if (version.useSampling()) {
                FDLogger.log(Level.FINE, "Enriching negative cover");
                FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
                FDLogger.log(Level.FINE, "Updating positive cover");
                inductor.updatePositiveCover(newNonFds);
            }
            FDLogger.log(Level.FINE, "Validating positive cover");
            comparisonSuggestions = validator.validatePositiveCover();
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Round " + i++);
        } while (comparisonSuggestions != null);

        // Return result
        int pruned = validator.getPruned();
        int validations = validator.getValidations();
        FDLogger.log(Level.FINE, "Pruned " + pruned + " validations");
        FDLogger.log(Level.FINE, "Made " + validations + " validations");
        List<FunctionalDependency> fds = new ArrayList<>();
        posCover.addFunctionalDependenciesInto(fds::add, this.buildColumnIdentifiers(), plis);
        SpeedBenchmark.end(BenchmarkLevel.METHOD_HIGH_LEVEL, "Processed one batch, inner measuring");
        return new IncrementalFDResult(fds, validations, pruned);
    }

    @SuppressWarnings("Duplicates")
    private IncrementalFDResult pruneDeleteBatch(CompressedDiff diff, List<PositionListIndex> plis, int[][] compressedRecords) throws AlgorithmExecutionException {

        // Idea: do not re-initialize negative cover, just take the old one & remove pos. invalidated records.
        FDSet negCover = new FDSet(columns.size(), -1);
        FDTree posCover = new FDTree(columns.size(), -1);
        List<OpenBitSet> affected = violationCollection.getAffected(negCover, diff.getDeletedRecords());

        Inductor inductor = new Inductor(negCover, posCover, this.memoryGuardian);
        FDLogger.log(Level.INFO, "Added " + inductor.addIntoPositiveCover(posCover, affected, columns.size()) + " generalisations to check");

        IncrementalValidator validator = new IncrementalValidator(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD, VALIDATE_PARALLEL, memoryGuardian);
        Sampler sampler = new Sampler(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD,
                intermediateDatastructure.getValueComparator(), this.memoryGuardian);


        // Actual algorithm execution
        List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        int i = 1;
        do {
            FDLogger.log(Level.FINE, "Started round " + i);
            FDLogger.log(Level.FINE, "Enriching negative cover");
            FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
            FDLogger.log(Level.FINE, "Updating positive cover");
            inductor.updatePositiveCover(newNonFds);
            FDLogger.log(Level.FINE, "Validating positive cover");
            comparisonSuggestions = validator.validatePositiveCover();
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Round " + i++);
        } while (comparisonSuggestions != null);

        List<FunctionalDependency> fds = new ArrayList<>();
        posCover.addFunctionalDependenciesInto(fds::add, this.buildColumnIdentifiers(), plis);
        int pruned = validator.getPruned();
        int validations = validator.getValidations();


        return new IncrementalFDResult(fds, validations, pruned);
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

}

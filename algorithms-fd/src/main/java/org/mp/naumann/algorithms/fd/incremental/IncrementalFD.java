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
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.datastructures.incremental.IncrementalDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.RecomputeDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.AllCombinationsBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.BloomPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.CurrentFDBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.pruning.simple.ExistingValuesPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolationCollection;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class IncrementalFD implements IncrementalAlgorithm<IncrementalFDResult, FDIntermediateDatastructure> {

    private static final boolean VALIDATE_PARALLEL = true;
    private static final float EFFICIENCY_THRESHOLD = 0.01f;
    private IncrementalFDConfiguration version = IncrementalFDConfiguration.LATEST;

    private List<String> columns;
    private FDTree posCover;
    private final String tableName;
    private final List<ResultListener<IncrementalFDResult>> resultListeners = new ArrayList<>();
    private MemoryGuardian memoryGuardian = new MemoryGuardian(true);

    private DataStructureBuilder dataStructureBuilder;
    private BloomPruningStrategy advancedBloomPruning;
    private ExistingValuesPruningStrategy simplePruning;
    private BloomPruningStrategy bloomPruning;
    private FDSet negCover;
    private ViolationCollection violationCollection;
    private ValueComparator valueComparator;

    public IncrementalFD(String tableName, IncrementalFDConfiguration version) {
        this(tableName);
        this.version = version;
    }


    public IncrementalFD(String tableName) {
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
    public void initialize(FDIntermediateDatastructure intermediateDatastructure) {
        FDLogger.log(Level.FINE, "Initializing IncrementalFD");
        this.posCover = intermediateDatastructure.getPosCover();
        this.negCover = intermediateDatastructure.getNegCover();
        this.violationCollection = intermediateDatastructure.getViolatingValues();

        PLIBuilder pliBuilder = intermediateDatastructure.getPliBuilder();
        this.valueComparator = intermediateDatastructure.getValueComparator();
        this.columns = intermediateDatastructure.getColumns();
        List<Integer> pliOrder = pliBuilder.getPliOrder();
        List<String> orderedColumns = pliOrder.stream().map(columns::get).collect(Collectors.toList());
        List<HashMap<String, IntArrayList>> clusterMaps = intermediateDatastructure.getPliBuilder().getClusterMaps();
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM)) {
            bloomPruning = new BloomPruningStrategy(orderedColumns).addGenerator(new AllCombinationsBloomGenerator(2));
            bloomPruning.initialize(clusterMaps, pliBuilder.getNumLastRecords(), pliOrder);
        }

        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM_ADVANCED)) {
            advancedBloomPruning = new BloomPruningStrategy(orderedColumns)
                    .addGenerator(new CurrentFDBloomGenerator(posCover));
            advancedBloomPruning.initialize(clusterMaps, pliBuilder.getNumLastRecords(), pliOrder);
        }
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
            simplePruning = new ExistingValuesPruningStrategy(columns);
        }
        if(version.recomputesDataStructures()) {
            dataStructureBuilder = new RecomputeDataStructureBuilder(pliBuilder, this.version, this.columns, pliOrder);
        } else {
            dataStructureBuilder = new IncrementalDataStructureBuilder(pliBuilder, this.version, this.columns, pliOrder);
        }
        FDLogger.log(Level.FINEST, violationCollection.toString());
    }

    @Override
    public IncrementalFDResult execute(Batch batch) throws AlgorithmExecutionException {

        FDLogger.log(Level.FINE, "Started IncrementalFD for new Batch");
        SpeedBenchmark.begin(BenchmarkLevel.METHOD_HIGH_LEVEL);

        CompressedDiff diff = dataStructureBuilder.update(batch);
        List<? extends PositionListIndex> plis = dataStructureBuilder.getPlis();
        CompressedRecords compressedRecords = dataStructureBuilder.getCompressedRecord();

        // Currently we don't have a version that handles both inserts and deletes well,
        // so we just 'escape' to handling only deletes once we have some.
        if(diff.getDeletedRecords().length > 0){
            return validateDeletes(diff, compressedRecords, plis);
        }
        IncrementalValidator validator = new IncrementalValidator(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD, VALIDATE_PARALLEL, memoryGuardian);
        IncrementalSampler sampler = new IncrementalSampler(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD,
                valueComparator, this.memoryGuardian);

        IncrementalInductor inductor = new IncrementalInductor(negCover, posCover, this.memoryGuardian);
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM)) {
            validator.addValidationPruner(bloomPruning.analyzeBatch(batch));
        }
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM_ADVANCED)) {
            validator.addValidationPruner(advancedBloomPruning.analyzeBatch(batch));
        }
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
            validator.addValidationPruner(simplePruning.analyzeDiff(diff));
        }

        FDLogger.log(Level.FINE, "Finished building pruning strategies");


        List<IntegerPair> comparisonSuggestions;
        int i = 1;
        do {
            FDLogger.log(Level.FINE, "Started round " + i);
            FDLogger.log(Level.FINE, "Validating positive cover");
            comparisonSuggestions = validator.validatePositiveCover();
            if (version.usesSampling() && comparisonSuggestions != null) {
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

    public IncrementalFDResult validateDeletes(CompressedDiff diff, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis) throws AlgorithmExecutionException {
        if(version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.ANNOTATION)){

            //FDTree posCover = new FDTree(columns.size(), -1);
            GeneralizingIncrementalValidator validator = new GeneralizingIncrementalValidator(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD, false, memoryGuardian);

            IncrementalInductor inductor = new IncrementalInductor(negCover, posCover, this.memoryGuardian);
            List<OpenBitSet> affected = violationCollection.getAffected(negCover, diff.getDeletedRecords());

            int induct = inductor.generalisePositiveCover(posCover, affected, violationCollection.getInvalidFds(), columns.size());
            FDLogger.log(Level.INFO, "Added " + induct + " candidates to check, depth now at "+posCover.getDepth());
            boolean theresmore;
            int i = 0;
            do{
                theresmore = validator.validatePositiveCover();
                SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Round " + i++);
            } while (theresmore);

            int pruned = validator.getPruned();
            int validations = validator.getValidations();
            FDLogger.log(Level.FINE, "Pruned " + pruned + " validations");
            FDLogger.log(Level.FINE, "Made " + validations + " validations");
            List<FunctionalDependency> fds = new ArrayList<>();
            posCover.addFunctionalDependenciesInto(fds::add, this.buildColumnIdentifiers(), plis);
            SpeedBenchmark.end(BenchmarkLevel.METHOD_HIGH_LEVEL, "Processed one batch, inner measuring");
            return new IncrementalFDResult(fds, validations, pruned);
        }
        return null;
    }

    private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.columns.size());
        for (String attributeName : this.columns)
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
        return columnIdentifiers;
    }

}

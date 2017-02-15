package org.mp.naumann.algorithms.fd.incremental.deprecated;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDIntermediateDatastructure;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.hyfd.FDList;
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDResult;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.datastructures.incremental.IncrementalDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.RecomputeDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.deprecated.validator.GeneralizingValidator;
import org.mp.naumann.algorithms.fd.incremental.deprecated.validator.SpecializingValidator;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.AllCombinationsBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.BloomPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.pruning.simple.ExistingValuesPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolationCollection;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
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

@Deprecated
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
        if(listener != null)
            this.resultListeners.add(listener);
    }

    @Override
    public void initialize(FDIntermediateDatastructure intermediateDatastructure) {
        FDLogger.log(Level.FINE, "Initializing IncrementalFD");
        this.columns = intermediateDatastructure.getColumns();
        this.valueComparator = intermediateDatastructure.getValueComparator();
        this.posCover = intermediateDatastructure.getPosCover();
        this.negCover = intermediateDatastructure.getNegCover();
        this.violationCollection = intermediateDatastructure.getViolatingValues();

        PLIBuilder pliBuilder = intermediateDatastructure.getPliBuilder();

        List<Integer> pliOrder = pliBuilder.getPliOrder();
        List<String> orderedColumns = pliOrder.stream().map(columns::get).collect(Collectors.toList());
        List<HashMap<String, IntArrayList>> clusterMaps = intermediateDatastructure.getPliBuilder().getClusterMaps();
        if(usesBloomPruning()) {
            bloomPruning = new BloomPruningStrategy(orderedColumns);
            if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM)) {
                bloomPruning.addGenerator(new AllCombinationsBloomGenerator(2));
            }
            if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM_ADVANCED)) {
//                bloomPruning.addGenerator(new CurrentFDBloomGenerator(posCover));
            }
            bloomPruning.initialize(clusterMaps, pliBuilder.getNumLastRecords(), pliOrder);
        }
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
            simplePruning = new ExistingValuesPruningStrategy(columns);
        }
        if(version.recomputesDataStructures()) {
            dataStructureBuilder = new RecomputeDataStructureBuilder(pliBuilder, this.version, this.columns, pliOrder);
        } else {
            dataStructureBuilder = new IncrementalDataStructureBuilder(pliBuilder, this.version, this.columns, pliOrder);
        }
        FDLogger.log(Level.FINEST, intermediateDatastructure.getViolatingValues().toString());
    }

    @Override
    public IncrementalFDResult execute(Batch batch) throws AlgorithmExecutionException {
        FDLogger.log(Level.FINE, "Started IncrementalFD for new Batch");
        SpeedBenchmark.begin(BenchmarkLevel.METHOD_HIGH_LEVEL);

        SpeedBenchmark.begin(BenchmarkLevel.UNIQUE);
        CompressedDiff diff = dataStructureBuilder.update(batch);
        SpeedBenchmark.end(BenchmarkLevel.UNIQUE, "BUILD DIFF");

        List<? extends PositionListIndex> plis = dataStructureBuilder.getPlis();
        CompressedRecords compressedRecords = dataStructureBuilder.getCompressedRecords();
        int validations = 0;
        if (!diff.getInsertedRecords().isEmpty()) {
            validations += validateTopDown(batch, diff, plis, compressedRecords);
        }
        if (!diff.getDeletedRecords().isEmpty()) {
            validations += validateBottomUp(diff, compressedRecords, plis);
        }
        List<FunctionalDependency> fds = new ArrayList<>();
        posCover.addFunctionalDependenciesInto(fds::add, this.buildColumnIdentifiers(), plis);
        SpeedBenchmark.end(BenchmarkLevel.METHOD_HIGH_LEVEL, "Processed one batch, inner measuring");
        return new IncrementalFDResult(fds, validations, 0);
    }

    private int validateTopDown(Batch batch, CompressedDiff diff, List<? extends PositionListIndex> plis, CompressedRecords compressedRecords) throws AlgorithmExecutionException {
        SpecializingValidator validator = new SpecializingValidator(version, negCover, posCover, dataStructureBuilder.getNumRecords(), compressedRecords, plis, EFFICIENCY_THRESHOLD, VALIDATE_PARALLEL, memoryGuardian);
        IncrementalSampler sampler = new IncrementalSampler(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD,
                valueComparator, this.memoryGuardian);

        IncrementalInductor inductor = new IncrementalInductor(negCover, posCover, this.memoryGuardian);
        if (usesBloomPruning()) {
            validator.addValidationPruner(bloomPruning.analyzeBatch(batch));
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
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Round " + i++);
        } while (comparisonSuggestions != null);
        // Return result
        int pruned = validator.getPruned();
        int validations = validator.getValidations();
        FDLogger.log(Level.FINE, "Pruned " + pruned + " validations");
        FDLogger.log(Level.FINE, "Made " + validations + " validations");
        return validations;
    }

    private boolean usesBloomPruning() {
        return version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM) || version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM_ADVANCED);
    }

    private int validateBottomUp(CompressedDiff diff, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis) throws AlgorithmExecutionException {
        if(version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.ANNOTATION)){


            //FDTree posCover = new FDTree(columns.size(), -1);
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Finished basic setup.");
              GeneralizingValidator validator = new GeneralizingValidator(version, negCover, posCover,
                    dataStructureBuilder.getNumRecords(), compressedRecords, plis, EFFICIENCY_THRESHOLD, VALIDATE_PARALLEL, memoryGuardian, violationCollection);

            IncrementalInductor inductor = new IncrementalInductor(negCover, posCover, this.memoryGuardian);
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Initialised valdiator and inductor");
            Collection<OpenBitSetFD> affected = violationCollection.getAffected(negCover, diff.getDeletedRecords().keySet());
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Received affected records");

            IncrementalSampler sampler = new IncrementalSampler(negCover, posCover, compressedRecords, plis, EFFICIENCY_THRESHOLD,
                    valueComparator, this.memoryGuardian);

            int induct = inductor.generalizePositiveCover(posCover, affected, violationCollection.getInvalidFds());
            FDLogger.log(Level.INFO, "Added " + induct + " candidates to check, depth now at "+posCover.getDepth());
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Inducted candidates into positive cover");
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
            return validations;
        }
        return 0;
    }

    private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.columns.size());
        for (String attributeName : this.columns)
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
        return columnIdentifiers;
    }

}

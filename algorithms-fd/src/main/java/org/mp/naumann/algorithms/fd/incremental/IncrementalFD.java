package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDIntermediateDatastructure;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.hyfd.FDList;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.Cluster;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.IntArrayListCluster;
import org.mp.naumann.algorithms.fd.incremental.pruning.Violations;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.ExactDeleteValidationPruner;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.SimpleDeleteValidationPruner;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.IncrementalValidator.ValidatorResult;
import org.mp.naumann.algorithms.fd.incremental.agreesets.AgreeSetCollection;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.datastructures.incremental.IncrementalDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.RecomputeDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.AllCombinationsBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.BloomPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.CurrentFDBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.pruning.simple.ExistingValuesPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.structures.Lattice;
import org.mp.naumann.algorithms.fd.incremental.structures.LatticeBuilder;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.processor.batch.Batch;

public class IncrementalFD implements IncrementalAlgorithm<IncrementalFDResult, FDIntermediateDatastructure> {

    private static final boolean USE_EXACT_PRUNER = false;
    private final List<ResultListener<IncrementalFDResult>> resultListeners = new ArrayList<>();
    private final String tableName;
    private boolean validateParallel = true;
    private float efficiencyThreshold = 0.01f;
    private IncrementalFDConfiguration version = IncrementalFDConfiguration.LATEST;
    private List<String> columns;
    private IntList pliOrder;
    private DataStructureBuilder dataStructureBuilder;
    private Lattice fds;
    private Lattice nonFds;
    private ValueComparator valueComparator;
    private ExistingValuesPruningStrategy simplePruning;
    private BloomPruningStrategy bloomPruning;
    private AgreeSetCollection agreeSets;
    private Violations violations = new Violations();

    public IncrementalFD(String tableName, IncrementalFDConfiguration version) {
        this(tableName);
        this.version = version;
    }

    public IncrementalFD(String tableName) {
        this.tableName = tableName;
    }

    public void setValidateParallel(boolean validateParallel) {
        this.validateParallel = validateParallel;
    }

    public void setEfficiencyThreshold(float efficiencyThreshold) {
        this.efficiencyThreshold = efficiencyThreshold;
    }

    @Override
    public Collection<ResultListener<IncrementalFDResult>> getResultListeners() {
        return resultListeners;
    }

    @Override
    public void addResultListener(ResultListener<IncrementalFDResult> listener) {
        if (listener != null) {
            this.resultListeners.add(listener);
        }
    }

    @Override
    public void initialize(FDIntermediateDatastructure intermediateDatastructure) {
        FDLogger.log(Level.INFO, "Initializing IncrementalFD");
        this.columns = intermediateDatastructure.getColumns();
        this.valueComparator = intermediateDatastructure.getValueComparator();

        PLIBuilder pliBuilder = intermediateDatastructure.getPliBuilder();
        this.pliOrder = pliBuilder.getPliOrder();

        List<OpenBitSetFD> functionalDependencies = intermediateDatastructure.getFunctionalDependencies();
        LatticeBuilder builder = LatticeBuilder.build(columns.size(), functionalDependencies);
        this.fds = builder.getFds();
        this.nonFds = builder.getNonFds();

//        Factory<Cluster> clusterFactory = pliBuilder.getNumLastRecords() > 1_000_000? IntOpenHashSet::new : IntArrayListCluster::new;
        Factory<Cluster> clusterFactory = IntArrayListCluster::new;

        if (version.recomputesDataStructures()) {
            dataStructureBuilder = new RecomputeDataStructureBuilder(pliBuilder, this.version,
                    this.columns, clusterFactory);
        } else {
            dataStructureBuilder = new IncrementalDataStructureBuilder(pliBuilder, this.version,
                    this.columns, clusterFactory);
        }

        this.agreeSets = intermediateDatastructure.getPruner();
        initializePruningStrategies(pliBuilder);
        FDLogger.log(Level.INFO, "Finished initializing IncrementalFD");
    }

    private void initializePruningStrategies(PLIBuilder pliBuilder) {
        if (usesBloomPruning()) {
            List<String> orderedColumns = pliOrder.stream().map(columns::get)
                    .collect(Collectors.toList());
            List<Map<String, IntList>> clusterMaps = pliBuilder.getClusterMaps();
            bloomPruning = new BloomPruningStrategy(orderedColumns);
            if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM)) {
                bloomPruning.addGenerator(new AllCombinationsBloomGenerator(3));
            }
            if (version
                    .usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM_ADVANCED)) {
                bloomPruning.addGenerator(new CurrentFDBloomGenerator(fds));
            }
            bloomPruning.initialize(clusterMaps, pliBuilder.getNumLastRecords(), pliOrder);
        }
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
            simplePruning = new ExistingValuesPruningStrategy(columns);
        }
        if (usesDeletePruning()) {
            List<? extends PositionListIndex> plis = dataStructureBuilder.getPlis();
            CompressedRecords compressedRecords = dataStructureBuilder.getCompressedRecords();
            NonFDInductor fdFinder = new NonFDInductor(fds, nonFds, plis,
                compressedRecords, dataStructureBuilder.getNumRecords(), efficiencyThreshold);
            NonFDValidator validator = new NonFDValidator(dataStructureBuilder.getNumRecords(),
                compressedRecords, plis, validateParallel, fds, nonFds, efficiencyThreshold, violations);
            validator.addPruningStrategy(PruningStrategy.DELETES);
            List<OpenBitSetFD> validFDs;
            do {
                try {
                    validFDs = validator.validate();
                } catch (AlgorithmExecutionException e) {
                    throw new RuntimeException(e);
                }
            } while (validFDs != null);
        }
    }

    private boolean usesBloomPruning() {
        return version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM)
                || version
                .usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM_ADVANCED);
    }

    private void prettyPrint(List<OpenBitSetFD> fds) {
        List<FunctionalDependency> pretty = getFunctionalDependencies(fds);
        pretty.forEach(System.out::println);
    }

    @Override
    public IncrementalFDResult execute(Batch batch) throws AlgorithmExecutionException {
        FDLogger.log(Level.INFO, "----");
        FDLogger.log(Level.INFO, "Started IncrementalFD for new Batch");
        Benchmark batchBenchmark = Benchmark.start("IncrementalFD for new Batch", BenchmarkLevel.BATCH.ordinal());
//        Benchmark benchmark = Benchmark.start("IncrementalFD for new Batch", BenchmarkLevel.BATCH.ordinal() + 1);

        FDLogger.log(Level.FINER, "Started updating data structures");
        CompressedDiff diff = dataStructureBuilder.update(batch);
//        benchmark.finishSubtask("Update data structures");
        List<? extends PositionListIndex> plis = dataStructureBuilder.getPlis();
        CompressedRecords compressedRecords = dataStructureBuilder.getCompressedRecords();

        int validations = 0;
        int pruned = 0;

        //is it better to delete first or insert first?
        //if delete first, FDs will move downwards. Likelihood that we delete all violations should be low
        //if insert first, FDs will move upwards. Likelihood that we introduce new violations should be high, especially if many values are retained
        if (diff.hasDeletes()) {
            ValidatorResult result = validateNonFDs(plis, compressedRecords, diff);
            validations += result.getValidations();
            pruned += result.getPruned();
//            benchmark.finishSubtask("Validate non-FDs");
        }

        if (diff.hasInserts()) {
            ValidatorResult result = validateFDs(plis, compressedRecords, batch, diff);
            validations += result.getValidations();
            pruned += result.getPruned();
//            benchmark.finishSubtask("Validate FDs");
        }

        List<OpenBitSetFD> fds = this.fds.getFunctionalDependencies();
        List<FunctionalDependency> result = getFunctionalDependencies(fds);
//        benchmark.finish();
        batchBenchmark.finish();

        return new IncrementalFDResult(result, validations, pruned);
    }

    private ValidatorResult validateFDs(List<? extends PositionListIndex> plis,
                                        CompressedRecords compressedRecords, Batch batch, CompressedDiff diff)
            throws AlgorithmExecutionException {
        FDLogger.log(Level.FINE, "Started validating FDs");
        Benchmark benchmark = Benchmark.start("Validate FDs", Benchmark.DEFAULT_LEVEL + 1);

        IncrementalMatcher matcher = new IncrementalMatcher(compressedRecords, valueComparator,
            agreeSets, version, violations);
        IncrementalSampler sampler = new IncrementalSampler(compressedRecords, plis,
                efficiencyThreshold, matcher);
        FDInductor inductor = new FDInductor(fds, nonFds,
                compressedRecords.getNumAttributes());
        FDValidator validator = new FDValidator(dataStructureBuilder.getNumRecords(),
                compressedRecords, plis, validateParallel, fds, nonFds, efficiencyThreshold, matcher, violations);

        if (usesBloomPruning()) {
            validator.addValidationPruner(bloomPruning.analyzeBatch(batch), PruningStrategy.BLOOM);
        }
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
            validator.addValidationPruner(simplePruning.analyzeDiff(diff), PruningStrategy.SIMPLE);
        }
        if (version.usesImprovedSampling()) {
            sampler.setNewRecords(diff.getInsertedRecords().keySet());
        }

        List<IntegerPair> comparisonSuggestions;
        int i = 1;
        do {
            Benchmark innerBenchmark = Benchmark
                    .start("Validate FDs Round " + i, Benchmark.DEFAULT_LEVEL + 2);
            FDLogger.log(Level.FINER, "Started round " + i);
            FDLogger.log(Level.FINER, "Validating positive cover");
            comparisonSuggestions = validator.validate();
            innerBenchmark.finishSubtask("Validation");
            if (version.usesSampling() && comparisonSuggestions != null) {
                FDLogger.log(Level.FINER, "Enriching agree sets");
                FDList agreeSets = sampler.enrichNegativeCover(comparisonSuggestions);
                innerBenchmark.finishSubtask("Sampling");
                FDLogger.log(Level.FINER, "Updating positive cover");
                int newFds = inductor.updatePositiveCover(agreeSets);
                innerBenchmark.finishSubtask("Inducted " + newFds + " new FDs");
            }
            innerBenchmark.finish();
            FDLogger.log(Level.FINER, "Finished round " + i++);
        } while (comparisonSuggestions != null);

        benchmark.finish();
        FDLogger.log(Level.FINE, "Finished validating FDs");
        return validator.getValidatorResult();
    }

    private ValidatorResult validateNonFDs(List<? extends PositionListIndex> plis,
                                           CompressedRecords compressedRecords, CompressedDiff diff)
            throws AlgorithmExecutionException {
        FDLogger.log(Level.FINE, "Started validating non-FDs");
        Benchmark benchmark = Benchmark.start("Validating non-FDs", Benchmark.DEFAULT_LEVEL + 1);
        NonFDInductor fdFinder = new NonFDInductor(fds, nonFds, plis,
                compressedRecords, dataStructureBuilder.getNumRecords(), efficiencyThreshold);
        NonFDValidator validator = new NonFDValidator(dataStructureBuilder.getNumRecords(),
                compressedRecords, plis, validateParallel, fds, nonFds, efficiencyThreshold, violations);
        if (usesDeletePruning()) {
            ValidationPruner pruner = violations.createPruner(diff.getDeletedRecords().keySet());
            validator.addValidationPruner(pruner, PruningStrategy.DELETES);
            benchmark.finishSubtask("Pruning");
        }
        if (version.usesPruningStrategy(PruningStrategy.DELETE_ANNOTATIONS)) {
            Set<OpenBitSet> agreeSets = this.agreeSets.analyzeDiff(diff);
            final ValidationPruner pruner;
            if (USE_EXACT_PRUNER) {
                pruner = new ExactDeleteValidationPruner(agreeSets, columns.size());
            } else{
                pruner = new SimpleDeleteValidationPruner(agreeSets);
            }
            validator.addValidationPruner(pruner, PruningStrategy.DELETE_ANNOTATIONS);
            benchmark.finishSubtask("Pruning");
        }
        List<OpenBitSetFD> validFDs;
        do {
            validFDs = validator.validate();
            if (version.usesDepthFirst() && validFDs != null) {
                fdFinder.findFDs(validFDs);
            }
        } while (validFDs != null);
        benchmark.finishSubtask("Validation");
        benchmark.finish();
        FDLogger.log(Level.FINE, "Finished validating non-FDs");
        return validator.getValidatorResult();
    }

    private boolean usesDeletePruning() {
        return version.usesPruningStrategy(PruningStrategy.DELETES);
    }

    private List<FunctionalDependency> getFunctionalDependencies(List<OpenBitSetFD> fds) {
        List<FunctionalDependency> result = new ArrayList<>(fds.size());
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = buildColumnIdentifiers();
        for (OpenBitSetFD fd : fds) {
            OpenBitSet lhs = fd.getLhs();
            ColumnIdentifier[] cols = new ColumnIdentifier[(int) fd.getLhs().cardinality()];
            int i = 0;
            for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0;
                 lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
                cols[i++] = columnIdentifiers.get(pliOrder.getInt(lhsAttr));
            }
            ColumnIdentifier rhs = columnIdentifiers.get(pliOrder.getInt(fd.getRhs()));
            result.add(new FunctionalDependency(new ColumnCombination(cols), rhs));
        }
        return result;
    }

    private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(
                this.columns.size());
        for (String attributeName : this.columns) {
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
        }
        return columnIdentifiers;
    }
}

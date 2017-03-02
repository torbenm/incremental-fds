package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.benchmark.better.Benchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDIntermediateDatastructure;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.hyfd.FDList;
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.IncrementalValidator.ValidatorResult;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.datastructures.incremental.IncrementalDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.RecomputeDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.DeletePruner;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.AllCombinationsBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.BloomPruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.pruning.bloom.CurrentFDBloomGenerator;
import org.mp.naumann.algorithms.fd.incremental.pruning.simple.ExistingValuesPruningStrategy;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.Lattice;
import org.mp.naumann.algorithms.fd.structures.LatticeBuilder;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class IncrementalFD implements IncrementalAlgorithm<IncrementalFDResult, FDIntermediateDatastructure> {

    private final List<ResultListener<IncrementalFDResult>> resultListeners = new ArrayList<>();
    private final String tableName;
    private boolean validateParallel = true;
    private float efficiencyThreshold = 0.01f;
    private IncrementalFDConfiguration version = IncrementalFDConfiguration.LATEST;
    private List<String> columns;
    private List<Integer> pliOrder;
    private DataStructureBuilder dataStructureBuilder;
    private Lattice fds;
    private Lattice nonFds;
    private FDSet agreeSets;
    private ValueComparator valueComparator;
    private ExistingValuesPruningStrategy simplePruning;
    private BloomPruningStrategy bloomPruning;
    private DeletePruner deletePruner;

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
        this.agreeSets = intermediateDatastructure.getNegCover();
        this.valueComparator = intermediateDatastructure.getValueComparator();

        PLIBuilder pliBuilder = intermediateDatastructure.getPliBuilder();
        this.pliOrder = pliBuilder.getPliOrder();

        FDTree posCover = intermediateDatastructure.getPosCover();
        LatticeBuilder builder = LatticeBuilder.build(posCover);
        this.fds = builder.getFds();
        this.nonFds = builder.getNonFds();

        if (version.recomputesDataStructures()) {
            dataStructureBuilder = new RecomputeDataStructureBuilder(pliBuilder, this.version, this.columns, this.pliOrder);
        } else {
            dataStructureBuilder = new IncrementalDataStructureBuilder(pliBuilder, this.version, this.columns, this.pliOrder);
        }

        this.deletePruner = intermediateDatastructure.getPruner();
        initializePruningStrategies(pliBuilder);
        FDLogger.log(Level.INFO, "Finished initializing IncrementalFD");
    }

    private void initializePruningStrategies(PLIBuilder pliBuilder) {
        if (usesBloomPruning()) {
            List<String> orderedColumns = pliOrder.stream().map(columns::get).collect(Collectors.toList());
            List<HashMap<String, IntArrayList>> clusterMaps = pliBuilder.getClusterMaps();
            bloomPruning = new BloomPruningStrategy(orderedColumns);
            if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM)) {
                bloomPruning.addGenerator(new AllCombinationsBloomGenerator(2));
            }
            if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM_ADVANCED)) {
                bloomPruning.addGenerator(new CurrentFDBloomGenerator(fds));
            }
            bloomPruning.initialize(clusterMaps, pliBuilder.getNumLastRecords(), pliOrder);
        }
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
            simplePruning = new ExistingValuesPruningStrategy(columns);
        }
    }

    private boolean usesBloomPruning() {
        return version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM) || version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.BLOOM_ADVANCED);
    }

    private void prettyPrint(List<OpenBitSetFD> fds) {
        List<FunctionalDependency> pretty = getFunctionalDependencies(fds);
        pretty.forEach(System.out::println);
    }

    @Override
    public IncrementalFDResult execute(Batch batch) throws AlgorithmExecutionException {
        FDLogger.log(Level.INFO, "Started IncrementalFD for new Batch");
        Benchmark benchmark = Benchmark.start("IncrementalFD for new Batch");

        FDLogger.log(Level.FINER, "Started updating data structures");
        CompressedDiff diff = dataStructureBuilder.update(batch);
        List<? extends PositionListIndex> plis = dataStructureBuilder.getPlis();
        CompressedRecords compressedRecords = dataStructureBuilder.getCompressedRecords();
        benchmark.finishSubtask("Update data structures");
        FDLogger.log(Level.FINER, "Finished updating data structures");

        int validations = 0;
        int pruned = 0;

        if (diff.hasInserts()) {
            ValidatorResult result = validateFDs(plis, compressedRecords, batch, diff);
            validations += result.getValidations();
            pruned += result.getPruned();
            benchmark.finishSubtask("Validate FDs");
        }

        if (diff.hasDeletes()) {
            ValidatorResult result = validateNonFDs(plis, compressedRecords, diff);
            validations += result.getValidations();
            pruned += result.getPruned();
            benchmark.finishSubtask("Validate non-FDs");
        }

        List<OpenBitSetFD> fds = this.fds.getFunctionalDependencies();
        List<FunctionalDependency> result = getFunctionalDependencies(fds);
        FDLogger.log(Level.INFO, "Finished IncrementalFD for new Batch");
        benchmark.finish();

//        this.fds.print();
        //this.nonFds.print();

        return new IncrementalFDResult(result, validations, pruned);
    }

    private ValidatorResult validateFDs(List<? extends PositionListIndex> plis, CompressedRecords compressedRecords, Batch batch, CompressedDiff diff) throws AlgorithmExecutionException {
        FDLogger.log(Level.FINE, "Started validating FDs");
        Benchmark benchmark = Benchmark.start("Validate FDs", Benchmark.DEFAULT_LEVEL + 1);

        IncrementalMatcher matcher = new IncrementalMatcher(compressedRecords, valueComparator, deletePruner, version);
        IncrementalSampler sampler = new IncrementalSampler(agreeSets, compressedRecords, plis, efficiencyThreshold, matcher);
        IncrementalInductor inductor = new IncrementalInductor(nonFds, fds, compressedRecords.getNumAttributes());
        IncrementalValidator validator = new FDValidator(dataStructureBuilder.getNumRecords(), compressedRecords, plis, validateParallel, fds, nonFds, efficiencyThreshold, matcher);

        if (usesBloomPruning()) {
            validator.addValidationPruner(bloomPruning.analyzeBatch(batch));
        }
        if (version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
            validator.addValidationPruner(simplePruning.analyzeDiff(diff));
        }
        if (version.usesImprovedSampling()) {
            sampler.setNewRecords(diff.getInsertedRecords().keySet());
        }

        List<IntegerPair> comparisonSuggestions;
        int i = 1;
        do {
            Benchmark innerBenchmark = Benchmark.start("Validate FDs Round " + i, Benchmark.DEFAULT_LEVEL + 2);
            FDLogger.log(Level.FINER, "Started round " + i);
            FDLogger.log(Level.FINER, "Validating positive cover");
            comparisonSuggestions = validator.validate();
            innerBenchmark.finishSubtask("Validation");
            if (version.usesSampling() && comparisonSuggestions != null) {
                FDLogger.log(Level.FINER, "Enriching agree sets");
                FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
                innerBenchmark.finishSubtask("Sampling");
                FDLogger.log(Level.FINER, "Updating positive cover");
                inductor.updatePositiveCover(newNonFds);
                innerBenchmark.finishSubtask("Inducing");
            }
            innerBenchmark.finish();
            FDLogger.log(Level.FINER, "Finished round " + i++);
        } while (comparisonSuggestions != null);

        benchmark.finish();
        FDLogger.log(Level.FINE, "Finished validating FDs");
        return validator.getValidatorResult();
    }

    private ValidatorResult validateNonFDs(List<? extends PositionListIndex> plis, CompressedRecords compressedRecords, CompressedDiff diff) throws AlgorithmExecutionException {
        FDLogger.log(Level.FINE, "Started validating non-FDs");
        Benchmark benchmark = Benchmark.start("Validating non-FDs", Benchmark.DEFAULT_LEVEL + 1);
        IncrementalValidator validator = new NonFDValidator(dataStructureBuilder.getNumRecords(), compressedRecords, plis, validateParallel, fds, nonFds);
        if (version.usesPruningStrategy(PruningStrategy.DELETE_ANNOTATIONS)) {
            ValidationPruner pruner = deletePruner.analyzeDiff(diff);
            validator.addValidationPruner(pruner);
            benchmark.finishSubtask("Pruning");
        }
        validator.validate();
        benchmark.finishSubtask("Validation");
        benchmark.finish();
        FDLogger.log(Level.FINE, "Finished validating non-FDs");
        return validator.getValidatorResult();
    }

    private List<FunctionalDependency> getFunctionalDependencies(List<OpenBitSetFD> fds) {
        List<FunctionalDependency> result = new ArrayList<>(fds.size());
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = buildColumnIdentifiers();
        for (OpenBitSetFD fd : fds) {
            OpenBitSet lhs = fd.getLhs();
            ColumnIdentifier[] cols = new ColumnIdentifier[(int) fd.getLhs().cardinality()];
            int i = 0;
            for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
                cols[i++] = columnIdentifiers.get(pliOrder.get(lhsAttr));
            }
            ColumnIdentifier rhs = columnIdentifiers.get(pliOrder.get(fd.getRhs()));
            result.add(new FunctionalDependency(new ColumnCombination(cols), rhs));
        }
        return result;
    }

    private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.columns.size());
        for (String attributeName : this.columns) {
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
        }
        return columnIdentifiers;
    }
}

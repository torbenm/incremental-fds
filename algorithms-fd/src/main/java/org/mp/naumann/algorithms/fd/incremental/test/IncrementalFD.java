package org.mp.naumann.algorithms.fd.incremental.test;

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
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDResult;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.RecomputeDataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.test.Validator.ValidatorResult;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;

public class IncrementalFD implements IncrementalAlgorithm<IncrementalFDResult, FDIntermediateDatastructure> {

    private static final boolean VALIDATE_PARALLEL = true;
    private static final float EFFICIENCY_THRESHOLD = 0.01f;
    private IncrementalFDConfiguration version = IncrementalFDConfiguration.LATEST;

    private List<String> columns;
    private final String tableName;
    private final List<ResultListener<IncrementalFDResult>> resultListeners = new ArrayList<>();

    private DataStructureBuilder dataStructureBuilder;
    private Lattice fds;
    private Lattice nonFds;
    private List<Integer> pliOrder;
    private FDSet agreeSets;
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
        this.agreeSets = intermediateDatastructure.getNegCover();
        this.valueComparator = intermediateDatastructure.getValueComparator();

        PLIBuilder pliBuilder = intermediateDatastructure.getPliBuilder();

        this.pliOrder = pliBuilder.getPliOrder();
        LatticeBuilder builder = LatticeBuilder.build(intermediateDatastructure.getPosCover());
        this.fds = builder.getFds();
        this.nonFds = builder.getNonFds();
        dataStructureBuilder = new RecomputeDataStructureBuilder(pliBuilder, this.version, this.columns, pliOrder);
    }

    private void prettyPrint(List<OpenBitSetFD> fds) {
        List<FunctionalDependency> pretty = getFunctionalDependencies(fds);
        pretty.forEach(System.out::println);
    }

    @Override
    public IncrementalFDResult execute(Batch batch) throws AlgorithmExecutionException {
        FDLogger.log(Level.FINE, "Started IncrementalFD for new Batch");
        SpeedBenchmark.begin(BenchmarkLevel.METHOD_HIGH_LEVEL);

        CompressedDiff diff = dataStructureBuilder.update(batch);

        List<? extends PositionListIndex> plis = dataStructureBuilder.getPlis();
        CompressedRecords compressedRecords = dataStructureBuilder.getCompressedRecord();
        int validations = 0;
        int pruned = 0;
        if (!diff.getInsertedRecords().isEmpty()) {
            ValidatorResult result = validateFDs(plis, compressedRecords);
            validations += result.getValidations();
            pruned += result.getPruned();
        }
        if (!diff.getDeletedRecords().isEmpty()) {
            ValidatorResult result = validateNonFDs(plis, compressedRecords);
            validations += result.getValidations();
            pruned += result.getPruned();
        }
        List<OpenBitSetFD> fds = this.fds.getFunctionalDependencies();
        List<FunctionalDependency> result = getFunctionalDependencies(fds);
        SpeedBenchmark.end(BenchmarkLevel.METHOD_HIGH_LEVEL, "Processed one batch, inner measuring");
        return new IncrementalFDResult(result, validations, pruned);
    }

    private ValidatorResult validateFDs(List<? extends PositionListIndex> plis, CompressedRecords compressedRecords) throws AlgorithmExecutionException {
        IncrementalSampler sampler = new IncrementalSampler(agreeSets, compressedRecords, plis, EFFICIENCY_THRESHOLD, valueComparator);
        Inductor inductor = new Inductor(nonFds, fds, pliOrder.size());
        Validator validator = new FDValidator(dataStructureBuilder.getNumRecords(), compressedRecords, plis, VALIDATE_PARALLEL, fds, nonFds, EFFICIENCY_THRESHOLD);

        List<IntegerPair> comparisonSuggestions;
        int i = 1;
        do {
            FDLogger.log(Level.FINE, "Started round " + i);
            FDLogger.log(Level.FINE, "Validating positive cover");
            comparisonSuggestions = validator.validate();
            if (version.usesSampling() && comparisonSuggestions != null) {
                FDLogger.log(Level.FINE, "Enriching negative cover");
                FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
                FDLogger.log(Level.FINE, "Updating positive cover");
                inductor.updatePositiveCover(newNonFds);
            }
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Round " + i++);
        } while (comparisonSuggestions != null);

        return validator.getValidatorResult();
    }

    private ValidatorResult validateNonFDs(List<? extends PositionListIndex> plis, CompressedRecords compressedRecords) throws AlgorithmExecutionException {
        Validator validator = new NonFDValidator(dataStructureBuilder.getNumRecords(), compressedRecords, plis, VALIDATE_PARALLEL, fds, nonFds);
        validator.validate();
        return validator.getValidatorResult();
    }

    private List<FunctionalDependency> getFunctionalDependencies(List<OpenBitSetFD> fds) {
        List<FunctionalDependency> result = new ArrayList<>(fds.size());
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = buildColumnIdentifiers();
        for (OpenBitSetFD fd : fds) {
            OpenBitSet lhs = fd.getLhs();
            ColumnIdentifier[] cols = new ColumnIdentifier[(int) fd.getLhs().cardinality()];
            int i = 0;
            for(int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
                cols[i++] = columnIdentifiers.get(pliOrder.get(lhsAttr));
            }
            ColumnIdentifier rhs = columnIdentifiers.get(pliOrder.get(fd.getRhs()));
            result.add(new FunctionalDependency(new ColumnCombination(cols), rhs));
        }
        return result;
    }

    private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.columns.size());
        for (String attributeName : this.columns)
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
        return columnIdentifiers;
    }
}

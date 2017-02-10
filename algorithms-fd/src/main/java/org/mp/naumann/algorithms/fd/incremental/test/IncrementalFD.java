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
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDResult;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.RecomputeDataStructureBuilder;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
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
    private IncrementalFDConfiguration version = IncrementalFDConfiguration.LATEST;

    private List<String> columns;
    private final String tableName;
    private final List<ResultListener<IncrementalFDResult>> resultListeners = new ArrayList<>();

    private DataStructureBuilder dataStructureBuilder;
    private Lattice validFds;
    private Lattice invalidFds;
    private List<Integer> pliOrder;

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

        PLIBuilder pliBuilder = intermediateDatastructure.getPliBuilder();

        this.pliOrder = pliBuilder.getPliOrder();
        LatticeBuilder builder = LatticeBuilder.build(intermediateDatastructure.getPosCover());
        this.validFds = builder.getValidFds();
        this.invalidFds = builder.getInvalidFds();
        List<OpenBitSetFD> nonFds = invalidFds.getFunctionalDependencies();
        List<OpenBitSetFD> fds = validFds.getFunctionalDependencies();
        nonFds.forEach(nonFd -> nonFd.getLhs().flip(0, pliOrder.size()));
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
        if (!diff.getInsertedRecords().isEmpty()) {
            Validator validator = new FDValidator(dataStructureBuilder.getNumRecords(), compressedRecords, plis, VALIDATE_PARALLEL, validFds, invalidFds);
            validator.validate();
            validations += validator.getValidations();
        }
        if (!diff.getDeletedRecords().isEmpty()) {
            Validator validator = new NonFDValidator(dataStructureBuilder.getNumRecords(), compressedRecords, plis, VALIDATE_PARALLEL, validFds, invalidFds);
            validator.validate();
            validations += validator.getValidations();
        }
        List<OpenBitSetFD> fds = validFds.getFunctionalDependencies();
        List<FunctionalDependency> result = getFunctionalDependencies(fds);
        SpeedBenchmark.end(BenchmarkLevel.METHOD_HIGH_LEVEL, "Processed one batch, inner measuring");
        return new IncrementalFDResult(result, validations, 0);
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

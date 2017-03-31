package org.mp.naumann.algorithms.fd.hyfd;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.incremental.agreesets.AgreeSetCollection;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.structures.RecordCompressor;
import org.mp.naumann.algorithms.fd.utils.FileUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;


public class HyFD implements FunctionalDependencyAlgorithm {

    private final MemoryGuardian memoryGuardian = new MemoryGuardian(true);
    private final IncrementalFDConfiguration configuration;
    public int lastValidationCount = 0;

    // but usually we are only interested in FDs
    // with lhs < some threshold (otherwise they
    // would not be useful for normalization,
    // key discovery etc.)
    private Table table = null;
    private FunctionalDependencyResultReceiver resultReceiver = null;
    private ValueComparator valueComparator;
    private String tableName;
    private List<String> attributeNames;
    private int numAttributes;
    private FDTree posCover;
    private PLIBuilder pliBuilder;
    private AgreeSetCollection pruner;

    public HyFD() {
        this.configuration = IncrementalFDConfiguration.LATEST;
    }

    public HyFD(IncrementalFDConfiguration configuration, Table table, FunctionalDependencyResultReceiver resultReceiver) {
        this.configuration = configuration;
        configure(table, resultReceiver);
    }

    public void configure(Table table, FunctionalDependencyResultReceiver resultReceiver) {
        this.table = table;
        this.resultReceiver = resultReceiver;

    }

    private void initialize(TableInput tableInput) {
        this.tableName = tableInput.getName();
        this.attributeNames = tableInput.getColumnNames();
        this.numAttributes = this.attributeNames.size();
        if (this.valueComparator == null)
            this.valueComparator = new ValueComparator(true);
    }

    public void execute() throws AlgorithmExecutionException {
        long startTime = System.currentTimeMillis();
        if (this.table == null)
            throw new IllegalStateException("No input generator set!");
        if (this.resultReceiver == null)
            throw new IllegalStateException("No result receiver set!");

        // this.executeFDEP();
        this.executeHyFD();

        FDLogger.log(Level.FINER, "Time: " + (System.currentTimeMillis() - startTime) + " ms");
    }

    private void executeHyFD() throws AlgorithmExecutionException {
        // Initialize

        FDLogger.log(Level.FINER, "Initializing ...");
        TableInput tableInput = this.getInput();
        this.initialize(tableInput);

        ///////////////////////////////////////////////////////
        // Build data structures for sampling and validation //
        ///////////////////////////////////////////////////////

        // Calculate plis
        FDLogger.log(Level.FINER, "Reading data and calculating plis ...");
        this.pliBuilder = new PLIBuilder(this.numAttributes, this.valueComparator.isNullEqualNull());
        pliBuilder.addRecords(tableInput);
        List<PositionListIndex> plis = pliBuilder.fetchPositionListIndexes();
        this.closeInput(tableInput);

        final int numRecords = pliBuilder.getNumLastRecords();

        if (numRecords == 0) {
            ObjectArrayList<ColumnIdentifier> columnIdentifiers = this.buildColumnIdentifiers();
            for (int attr = 0; attr < this.numAttributes; attr++)
                this.resultReceiver
                        .receiveResult(new FunctionalDependency(new ColumnCombination(), columnIdentifiers.get(attr)));
            return;
        }

        int[][] compressedRecords = RecordCompressor.fetchCompressedRecords(plis, numRecords);
        // Initialize the negative cover
        int maxLhsSize = -1;
        FDSet negCover = new FDSet(this.numAttributes, maxLhsSize);

        // Initialize the positive cover
        FDTree posCover = new FDTree(this.numAttributes, maxLhsSize);
        posCover.addMostGeneralDependencies();
        //////////////////////////
        // Build the components //
        //////////////////////////


        // TODO: implement parallel sampling

        float efficiencyThreshold = 0.01f;
        Matcher matcher = new Matcher(compressedRecords, valueComparator, configuration);
        Sampler sampler = new Sampler(negCover, posCover, compressedRecords, plis, efficiencyThreshold,
                this.memoryGuardian, matcher);
        Inductor inductor = new Inductor(negCover, posCover, this.memoryGuardian);
        boolean validateParallel = true;
        Validator validator = new Validator(negCover, posCover, numRecords, compressedRecords, plis,
                efficiencyThreshold, validateParallel, this.memoryGuardian, matcher);

        List<IntegerPair> comparisonSuggestions = new ArrayList<>();

        int i = 1;
        lastValidationCount = 0;
        do {
            FDLogger.log(Level.FINE, "Started round " + i);
            FDLogger.log(Level.FINE, "Enriching negative cover");
            FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
            FDLogger.log(Level.FINE, "Updating positive cover");
            inductor.updatePositiveCover(newNonFds);
            FDLogger.log(Level.FINE, "Validating positive cover");
            comparisonSuggestions = validator.validatePositiveCover();
            lastValidationCount += validator.lastValidationCount;
        } while (comparisonSuggestions != null);

        //violationCollection.print();
        // Output all valid FDs
        FDLogger.log(Level.FINER, "Translating FD-tree into result format ...");

        // int numFDs = posCover.writeFunctionalDependencies("HyFD_backup_" +
        // this.tableName + "_results.txt", this.buildColumnIdentifiers(), plis,
        // false);
        int numFDs = posCover.addFunctionalDependenciesInto(this.resultReceiver, this.buildColumnIdentifiers(), plis);

        FDLogger.log(Level.FINER, "... done! (" + numFDs + " FDs)");

        this.posCover = posCover;
        this.pruner = matcher.getAgreeSets();
    }

    public FDTree getPosCover() {
        return posCover;
    }

    private TableInput getInput() {
        try {

            return this.table.open();

        } catch (InputReadException e) {
            throw new RuntimeException("Input generation failed!", e);
        }
    }


    private void closeInput(TableInput tableInput) {
        FileUtils.close(tableInput);
    }

    private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.attributeNames.size());
        for (String attributeName : this.attributeNames)
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
        return columnIdentifiers;
    }

    public PLIBuilder getPLIBuilder() {
        return pliBuilder;
    }

    public ValueComparator getValueComparator() {
        return valueComparator;
    }

    public List<String> getColumns() {
        return attributeNames;
    }

    public AgreeSetCollection getPruner() {
        return pruner;
    }
}

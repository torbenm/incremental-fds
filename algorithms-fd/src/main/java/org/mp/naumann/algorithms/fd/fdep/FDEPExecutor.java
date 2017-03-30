package org.mp.naumann.algorithms.fd.fdep;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.hyfd.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.RecordCompressor;
import org.mp.naumann.algorithms.fd.utils.FileUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.data.Row;

import java.util.List;
import java.util.logging.Level;

public class FDEPExecutor implements FunctionalDependencyAlgorithm {

    private Table table = null;
    private FunctionalDependencyResultReceiver resultReceiver = null;

    private ValueComparator valueComparator;

    private String tableName;
    private List<String> attributeNames;
    private int numAttributes;

    public FDEPExecutor() {

    }

    public FDEPExecutor(Table table, FunctionalDependencyResultReceiver resultReceiver) {
        this();
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

    public void execute() {
        long startTime = System.currentTimeMillis();

        this.executeFDEP();

        FDLogger.log(Level.FINEST, "Time: " + (System.currentTimeMillis() - startTime) + " ms");
    }

    private void executeFDEP() {
        // Initialize
        FDLogger.log(Level.FINEST, "Initializing ...");
        TableInput tableInput = this.getInput();
        this.initialize(tableInput);

        // Load data
        FDLogger.log(Level.FINEST, "Loading data ...");
        ObjectArrayList<Row> records = this.loadData(tableInput);
        this.closeInput(tableInput);

        // Create default output if input is empty
        if (records.isEmpty()) {
            ObjectArrayList<ColumnIdentifier> columnIdentifiers = this.buildColumnIdentifiers();
            for (int attr = 0; attr < this.numAttributes; attr++)
                this.resultReceiver.receiveResult(new FunctionalDependency(new ColumnCombination(), columnIdentifiers.get(attr)));
            return;
        }

        int numRecords = records.size();

        // Calculate plis
        FDLogger.log(Level.FINEST, "Calculating plis ...");
        PLIBuilder pliBuilder = new PLIBuilder(this.numAttributes, this.valueComparator.isNullEqualNull());
        pliBuilder.addRecords(records);
        List<PositionListIndex> plis = pliBuilder.fetchPositionListIndexes();
        pliBuilder = null;
        records = null; // we proceed with the values in the plis

        int[][] compressedRecords = RecordCompressor.fetchCompressedRecords(plis, numRecords);

        // Execute fdep
        FDLogger.log(Level.FINEST, "Executing fdep ...");
        FDEP fdep = new FDEP(this.numAttributes, this.valueComparator);
        FDTree fds = fdep.execute(compressedRecords);

        // Output all valid FDs
        FDLogger.log(Level.FINEST, "Translating fd-tree into result format ...");
        List<FunctionalDependency> result = fds.getFunctionalDependencies(this.buildColumnIdentifiers(), plis);
        plis = null;
        int numFDs = 0;
        for (FunctionalDependency fd : result) {
            //FDLogger.log(Level.FINEST, fd);
            this.resultReceiver.receiveResult(fd);
            numFDs++;
        }
        FDLogger.log(Level.FINEST, "... done! (" + numFDs + " FDs)");
    }

    private TableInput getInput() {
        try {
            return this.table.open();
        } catch (InputReadException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load input!", e);
        }
    }

    private void closeInput(TableInput relationalInput) {
        FileUtils.close(relationalInput);
    }

    private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.attributeNames.size());
        for (String attributeName : this.attributeNames)
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
        return columnIdentifiers;
    }

    private ObjectArrayList<Row> loadData(TableInput tableInput) {
        ObjectArrayList<Row> records = new ObjectArrayList<>();
        while (tableInput.hasNext())
            records.add(tableInput.next());
        return records;
    }


}

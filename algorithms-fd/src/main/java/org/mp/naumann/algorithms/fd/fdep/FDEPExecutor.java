package org.mp.naumann.algorithms.fd.fdep;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.FileUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.database.data.Row;


import java.util.List;

public class FDEPExecutor implements FunctionalDependencyAlgorithm {

	private Table table = null;
	private FunctionalDependencyResultReceiver resultReceiver = null;

	private ValueComparator valueComparator;
	
	private String tableName;
	private List<String> attributeNames;
	private int numAttributes;

	public FDEPExecutor(Table table, FunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
		this.table = table;
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
		
		System.out.println("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private void executeFDEP() {
		// Initialize
		System.out.println("Initializing ...");
		TableInput tableInput = this.getInput();
		this.initialize(tableInput);
		
		// Load data
		System.out.println("Loading data ...");
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
		System.out.println("Calculating plis ...");
		List<PositionListIndex> plis = PLIBuilder.getPLIs(records, this.numAttributes, this.valueComparator.isNullEqualNull());
		records = null; // we proceed with the values in the plis
		
		// Calculate inverted plis
		System.out.println("Inverting plis ...");
		int[][] invertedPlis = PliUtils.invert(plis, numRecords);

		// Extract the integer representations of all records from the inverted plis
		System.out.println("Extracting integer representations for the records ...");
		int[][] compressedRecords = new int[numRecords][];
		for (int recordId = 0; recordId < numRecords; recordId++)
			compressedRecords[recordId] = this.fetchRecordFrom(recordId, invertedPlis);
		
		// Execute fdep
		System.out.println("Executing fdep ...");
		FDEP fdep = new FDEP(this.numAttributes, this.valueComparator);
		FDTree fds = fdep.execute(compressedRecords);
		
		// Output all valid FDs
		System.out.println("Translating fd-tree into result format ...");
		List<FunctionalDependency> result = fds.getFunctionalDependencies(this.buildColumnIdentifiers(), plis);
		plis = null;
		int numFDs = 0;
		for (FunctionalDependency fd : result) {
			//System.out.println(fd);
			this.resultReceiver.receiveResult(fd);
			numFDs++;
		}
		System.out.println("... done! (" + numFDs + " FDs)");
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

	
	private int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
		int[] record = new int[this.numAttributes];
		for (int i = 0; i < this.numAttributes; i++)
			record[i] = invertedPlis[i][recordId];
		return record;
	}
}

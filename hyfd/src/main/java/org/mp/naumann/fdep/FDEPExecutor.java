package org.mp.naumann.fdep;

import java.util.Arrays;
import java.util.List;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.ColumnCombination;
import org.mp.naumann.algorithms.ColumnIdentifier;
import org.mp.naumann.algorithms.FunctionalDependency;
import org.mp.naumann.algorithms.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.RelationalInput;
import org.mp.naumann.algorithms.RelationalInputGenerator;
import org.mp.naumann.structures.FDTree;
import org.mp.naumann.structures.PLIBuilder;
import org.mp.naumann.structures.PositionListIndex;
import org.mp.naumann.utils.FileUtils;
import org.mp.naumann.utils.ValueComparator;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class FDEPExecutor {

	public enum Identifier {
		INPUT_GENERATOR, NULL_EQUALS_NULL, VALIDATE_PARALLEL, ENABLE_MEMORY_GUARDIAN, MAX_DETERMINANT_SIZE
	};

	private RelationalInputGenerator inputGenerator = null;
	private FunctionalDependencyResultReceiver resultReceiver = null;

	private ValueComparator valueComparator;
	
	private String tableName;
	private List<String> attributeNames;
	private int numAttributes;

	public FDEPExecutor(RelationalInputGenerator inputGenerator, FunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
		this.inputGenerator = inputGenerator;
	}

	public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}

	private void initialize(RelationalInput relationalInput) throws AlgorithmExecutionException {
		this.tableName = relationalInput.relationName();
		this.attributeNames = relationalInput.columnNames();
		this.numAttributes = this.attributeNames.size();
		if (this.valueComparator == null)
			this.valueComparator = new ValueComparator(true);
	}
	
	public void execute() throws AlgorithmExecutionException {
		long startTime = System.currentTimeMillis();
		
		this.executeFDEP();
		
		System.out.println("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private void executeFDEP() throws AlgorithmExecutionException {
		// Initialize
		System.out.println("Initializing ...");
		RelationalInput relationalInput = this.getInput();
		this.initialize(relationalInput);
		
		// Load data
		System.out.println("Loading data ...");
		ObjectArrayList<List<String>> records = this.loadData(relationalInput);
		this.closeInput(relationalInput);
		
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
		int[][] invertedPlis = this.invertPlis(plis, numRecords);

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
	
	private RelationalInput getInput() {
		RelationalInput relationalInput = this.inputGenerator.generateNewCopy();
		return relationalInput;
	}
	
	private void closeInput(RelationalInput relationalInput) {
		FileUtils.close(relationalInput);
	}

	private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(this.attributeNames.size());
		for (String attributeName : this.attributeNames)
			columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
		return columnIdentifiers;
	}

	private ObjectArrayList<List<String>> loadData(RelationalInput relationalInput) {
		ObjectArrayList<List<String>> records = new ObjectArrayList<List<String>>();
		while (relationalInput.hasNext())
			records.add(relationalInput.next());
		return records;
	}

	private int[][] invertPlis(List<PositionListIndex> plis, int numRecords) {
		int[][] invertedPlis = new int[plis.size()][];
		for (int attr = 0; attr < plis.size(); attr++) {
			int[] invertedPli = new int[numRecords];
			Arrays.fill(invertedPli, -1);
			
			for (int clusterId = 0; clusterId < plis.get(attr).size(); clusterId++) {
				for (int recordId : plis.get(attr).getClusters().get(clusterId))
					invertedPli[recordId] = clusterId;
			}
			invertedPlis[attr] = invertedPli;
		}
		return invertedPlis;
	}
	
	private int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
		int[] record = new int[this.numAttributes];
		for (int i = 0; i < this.numAttributes; i++)
			record[i] = invertedPlis[i][recordId];
		return record;
	}
}

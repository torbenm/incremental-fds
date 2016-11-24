package org.mp.naumann.algorithms.fd.hyfd;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDLogger;
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
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.FileUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;


public class HyFD implements FunctionalDependencyAlgorithm {

    private Table table = null;
	private FunctionalDependencyResultReceiver resultReceiver = null;

	private ValueComparator valueComparator;
	private final MemoryGuardian memoryGuardian = new MemoryGuardian(true);

	// but usually we are only interested in FDs
									// with lhs < some threshold (otherwise they
									// would not be useful for normalization,
									// key discovery etc.)

	private String tableName;
	private List<String> attributeNames;
	private int numAttributes;

    public HyFD(){
        FDLogger.setCurrentAlgorithm(this);
    }

	public HyFD(Table table, FunctionalDependencyResultReceiver resultReceiver) {
        this();
        configure(table, resultReceiver);
	}

	public void configure(Table table, FunctionalDependencyResultReceiver resultReceiver){
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

		FDLogger.logln(Level.INFO,"Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private void executeHyFD() throws AlgorithmExecutionException {
		// Initialize
		FDLogger.logln(Level.INFO,"Initializing ...");
		TableInput tableInput = this.getInput();
		this.initialize(tableInput);

		///////////////////////////////////////////////////////
		// Build data structures for sampling and validation //
		///////////////////////////////////////////////////////

		// Calculate plis
		FDLogger.logln(Level.INFO,"Reading data and calculating plis ...");
		PLIBuilder pliBuilder = new PLIBuilder();
		List<PositionListIndex> plis = pliBuilder.getPLIs(tableInput, this.numAttributes,
				this.valueComparator.isNullEqualNull());
		this.closeInput(tableInput);

		final int numRecords = pliBuilder.getNumLastRecords();
		pliBuilder = null;

		if (numRecords == 0) {
			ObjectArrayList<ColumnIdentifier> columnIdentifiers = this.buildColumnIdentifiers();
			for (int attr = 0; attr < this.numAttributes; attr++)
				this.resultReceiver
						.receiveResult(new FunctionalDependency(new ColumnCombination(), columnIdentifiers.get(attr)));
			return;
		}

		// Sort plis by number of clusters: For searching in the covers and for
		// validation, it is good to have attributes with few non-unique values
		// and many clusters left in the prefix tree
		FDLogger.logln(Level.INFO,"Sorting plis by number of clusters ...");
		Collections.sort(plis, new Comparator<PositionListIndex>() {

			@Override
			public int compare(PositionListIndex o1, PositionListIndex o2) {
				int numClustersInO1 = numRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
				int numClustersInO2 = numRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
				return numClustersInO2 - numClustersInO1;
			}
		});

		// Calculate inverted plis
		FDLogger.logln(Level.INFO,"Inverting plis ...");
		int[][] invertedPlis = PliUtils.invert(plis, numRecords);

		// Extract the integer representations of all records from the inverted
		// plis
		FDLogger.logln(Level.INFO,"Extracting integer representations for the records ...");
		int[][] compressedRecords = new int[numRecords][];
		for (int recordId = 0; recordId < numRecords; recordId++)
			compressedRecords[recordId] = this.fetchRecordFrom(recordId, invertedPlis);
		invertedPlis = null;

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
		Sampler sampler = new Sampler(negCover, posCover, compressedRecords, plis, efficiencyThreshold,
				this.valueComparator, this.memoryGuardian);
		Inductor inductor = new Inductor(negCover, posCover, this.memoryGuardian);
		boolean validateParallel = true;
		Validator validator = new Validator(negCover, posCover, numRecords, compressedRecords, plis,
				efficiencyThreshold, validateParallel, this.memoryGuardian);

		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		do {
			FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
			inductor.updatePositiveCover(newNonFds);
			comparisonSuggestions = validator.validatePositiveCover();
		} while (comparisonSuggestions != null);
		negCover = null;

		// Output all valid FDs
		FDLogger.logln(Level.INFO,"Translating FD-tree into result format ...");

		// int numFDs = posCover.writeFunctionalDependencies("HyFD_backup_" +
		// this.tableName + "_results.txt", this.buildColumnIdentifiers(), plis,
		// false);
		int numFDs = posCover.addFunctionalDependenciesInto(this.resultReceiver, this.buildColumnIdentifiers(), plis);

		FDLogger.logln(Level.INFO,"... done! (" + numFDs + " FDs)");
	}

	private TableInput getInput() {
		try {

			return this.table.open();

		} catch (InputReadException e) {
			throw new RuntimeException("Input generation failed!",e);
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

	private int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
		int[] record = new int[this.numAttributes];
		for (int i = 0; i < this.numAttributes; i++)
			record[i] = invertedPlis[i][recordId];
		return record;
	}
}

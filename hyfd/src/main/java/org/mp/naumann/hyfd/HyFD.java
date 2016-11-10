package org.mp.naumann.hyfd;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.ColumnCombination;
import org.mp.naumann.algorithms.ColumnIdentifier;
import org.mp.naumann.algorithms.FunctionalDependency;
import org.mp.naumann.algorithms.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.RelationalInput;
import org.mp.naumann.algorithms.RelationalInputGenerator;
import org.mp.naumann.structures.FDTree;
import org.mp.naumann.structures.IntegerPair;
import org.mp.naumann.structures.PLIBuilder;
import org.mp.naumann.structures.PositionListIndex;
import org.mp.naumann.utils.FileUtils;
import org.mp.naumann.utils.ValueComparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HyFD {

	private RelationalInputGenerator inputGenerator = null;
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

	public HyFD(RelationalInputGenerator inputGenerator, FunctionalDependencyResultReceiver resultReceiver) {
		this.inputGenerator = inputGenerator;
		this.resultReceiver = resultReceiver;
	}

	private void initialize(RelationalInput relationalInput) {
		this.tableName = relationalInput.relationName();
		this.attributeNames = relationalInput.columnNames();
		this.numAttributes = this.attributeNames.size();
		if (this.valueComparator == null)
			this.valueComparator = new ValueComparator(true);
	}

	public void execute() throws AlgorithmExecutionException {
		long startTime = System.currentTimeMillis();
		if (this.inputGenerator == null)
			throw new IllegalStateException("No input generator set!");
		if (this.resultReceiver == null)
			throw new IllegalStateException("No result receiver set!");

		// this.executeFDEP();
		this.executeHyFD();

		System.out.println("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private void executeHyFD() throws AlgorithmExecutionException {
		// Initialize
		System.out.println("Initializing ...");
		RelationalInput relationalInput = this.getInput();
		this.initialize(relationalInput);

		///////////////////////////////////////////////////////
		// Build data structures for sampling and validation //
		///////////////////////////////////////////////////////

		// Calculate plis
		System.out.println("Reading data and calculating plis ...");
		PLIBuilder pliBuilder = new PLIBuilder();
		List<PositionListIndex> plis = pliBuilder.getPLIs(relationalInput, this.numAttributes,
				this.valueComparator.isNullEqualNull());
		this.closeInput(relationalInput);

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
		System.out.println("Sorting plis by number of clusters ...");
		Collections.sort(plis, new Comparator<PositionListIndex>() {

			@Override
			public int compare(PositionListIndex o1, PositionListIndex o2) {
				int numClustersInO1 = numRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
				int numClustersInO2 = numRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
				return numClustersInO2 - numClustersInO1;
			}
		});

		// Calculate inverted plis
		System.out.println("Inverting plis ...");
		int[][] invertedPlis = this.invertPlis(plis, numRecords);

		// Extract the integer representations of all records from the inverted
		// plis
		System.out.println("Extracting integer representations for the records ...");
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
		System.out.println("Translating FD-tree into result format ...");

		// int numFDs = posCover.writeFunctionalDependencies("HyFD_backup_" +
		// this.tableName + "_results.txt", this.buildColumnIdentifiers(), plis,
		// false);
		int numFDs = posCover.addFunctionalDependenciesInto(this.resultReceiver, this.buildColumnIdentifiers(), plis);

		System.out.println("... done! (" + numFDs + " FDs)");
	}

	private RelationalInput getInput() {
		RelationalInput relationalInput = this.inputGenerator.generateNewCopy();
		if (relationalInput == null)
			throw new RuntimeException("Input generation failed!");
		return relationalInput;
	}

	private void closeInput(RelationalInput relationalInput) {
		FileUtils.close(relationalInput);
	}

	private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(this.attributeNames.size());
		for (String attributeName : this.attributeNames)
			columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
		return columnIdentifiers;
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

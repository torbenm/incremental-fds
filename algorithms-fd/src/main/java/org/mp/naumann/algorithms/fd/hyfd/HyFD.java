package org.mp.naumann.algorithms.fd.hyfd;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.violations.SingleValueViolationCollection;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolationCollection;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.RecordCompressor;
import org.mp.naumann.algorithms.fd.utils.FileUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;

import java.util.ArrayList;
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

	private FDTree posCover;

	private FDSet negCover;
	private PLIBuilder pliBuilder;
	private final IncrementalFDConfiguration configuration;


    private final ViolationCollection violationCollection;

    public HyFD(){
        this.configuration = IncrementalFDConfiguration.LATEST;
        violationCollection = configuration.createViolationCollection();
    }

	public HyFD(IncrementalFDConfiguration configuration, Table table, FunctionalDependencyResultReceiver resultReceiver) {
        FDLogger.setCurrentAlgorithm(this);
        this.configuration = configuration;
        violationCollection = configuration.createViolationCollection();
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

		FDLogger.log(Level.FINER, "Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private void executeHyFD() throws AlgorithmExecutionException {
		// Initialize
        SpeedBenchmark.begin(BenchmarkLevel.OPERATION);
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
        SpeedBenchmark.lap(BenchmarkLevel.OPERATION, "Initialized Datastructures.");

		int[][] compressedRecords = RecordCompressor.fetchCompressedRecords(plis, numRecords);
		// Initialize the negative cover
		int maxLhsSize = -1;
		FDSet negCover = new FDSet(this.numAttributes, maxLhsSize);

		// Initialize the positive cover
		FDTree posCover = new FDTree(this.numAttributes, maxLhsSize);
		posCover.addMostGeneralDependencies();
        SpeedBenchmark.lap(BenchmarkLevel.OPERATION, "Calculated Negative and Positive Cover");
		//////////////////////////
		// Build the components //
		//////////////////////////



		// TODO: implement parallel sampling

		float efficiencyThreshold = 0.01f;
		Sampler sampler = new Sampler(configuration, negCover, posCover, compressedRecords, plis, efficiencyThreshold,
				this.valueComparator, this.memoryGuardian, violationCollection);
		Inductor inductor = new Inductor(negCover, posCover, this.memoryGuardian);
		boolean validateParallel = true;
		Validator validator = new Validator(negCover, posCover, numRecords, compressedRecords, plis,
				efficiencyThreshold, validateParallel, this.memoryGuardian, violationCollection);

		List<IntegerPair> comparisonSuggestions = new ArrayList<>();

        SpeedBenchmark.lap(BenchmarkLevel.OPERATION, "Initialised Sampler, Inductor and Validator");
        SpeedBenchmark.begin(BenchmarkLevel.METHOD_HIGH_LEVEL);
        int i = 1;
		do {
			FDLogger.log(Level.FINE, "Started round " + i);
			FDLogger.log(Level.FINE, "Enriching negative cover");
			FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
			FDLogger.log(Level.FINE, "Updating positive cover");
			inductor.updatePositiveCover(newNonFds);
			FDLogger.log(Level.FINE, "Validating positive cover");
			comparisonSuggestions = validator.validatePositiveCover();
            SpeedBenchmark.lap(BenchmarkLevel.METHOD_HIGH_LEVEL, "Round "+i++);
		} while (comparisonSuggestions != null);

        //violationCollection.print();
        // Output all valid FDs
		FDLogger.log(Level.FINER, "Translating FD-tree into result format ...");

		// int numFDs = posCover.writeFunctionalDependencies("HyFD_backup_" +
		// this.tableName + "_results.txt", this.buildColumnIdentifiers(), plis,
		// false);
		int numFDs = posCover.addFunctionalDependenciesInto(this.resultReceiver, this.buildColumnIdentifiers(), plis);
		
        SpeedBenchmark.end(BenchmarkLevel.OPERATION, "Translated FD-tree into result format");
		FDLogger.log(Level.FINER, "... done! (" + numFDs + " FDs)");
		
		this.posCover = posCover;
		this.negCover = negCover;
	}

	public FDTree getPosCover() {
		return posCover;
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

    public ViolationCollection getViolationCollection() {
        return violationCollection;
    }

	public FDSet getNegCover() {
		return negCover;
	}

	public PLIBuilder getPLIBuilder() {
		return pliBuilder;
	}

	public ValueComparator getValueComparator() {
		return valueComparator;
	}

}

package org.mp.naumann.algorithms.fd.hyfd;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.hyfd.validation.ParallelValidator;
import org.mp.naumann.algorithms.fd.hyfd.validation.Validator;
import org.mp.naumann.algorithms.fd.structures.FDList;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.TableInputProvider;
import org.mp.naumann.algorithms.fd.structures.plis.PliCollection;
import org.mp.naumann.algorithms.fd.utils.MemoryGuardian;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class HyFD implements FunctionalDependencyAlgorithm {

    private final static Logger LOG = Logger.getLogger(HyFD.class.getName());

    private final MemoryGuardian memoryGuardian = new MemoryGuardian(true);
    private final int MAX_LHS = -1;
    private final float EFFICIENCY_THRESHOLD = 0.01f;

    private final TableInputProvider tableInputProvider;
	private final FunctionalDependencyResultReceiver resultReceiver;
	private final ValueComparator valueComparator = new ValueComparator(true);

	public HyFD(Table table, FunctionalDependencyResultReceiver resultReceiver) {
        tableInputProvider = new TableInputProvider(table);
		this.resultReceiver = resultReceiver;
        if (this.resultReceiver == null)
            throw new IllegalStateException("No result receiver set!");
	}

	public void execute() throws AlgorithmExecutionException {
		long startTime = System.currentTimeMillis();
		this.executeHyFD();
		LOG.info("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private void executeHyFD() throws AlgorithmExecutionException {
        LOG.info("Intializing...");
        if (tableInputProvider.getTable().getRowCount() == 0) {
            informAboutAnyFD();
            return;
        }

        PliCollection plis = calculatePliCollection(initializeTableInput());

        //Create negative and positive cover
        FDSet negCover = buildNegativeCover();
        FDTree posCover = buildPositiveCover();

        //Build components
		Sampler sampler = new Sampler(negCover, posCover, plis, EFFICIENCY_THRESHOLD,
				this.valueComparator, this.memoryGuardian);

		Inductor inductor = new Inductor(negCover, posCover, this.memoryGuardian);

		Validator validator = new ParallelValidator(negCover, posCover, plis,
				EFFICIENCY_THRESHOLD, this.memoryGuardian);

        //calculate fds
        calculateFDs(sampler, validator, inductor);

		// Output all valid FDs
		LOG.info("Translating OpenBitFunctionalDependency-tree into result format ...");
		int numFDs = informAboutFDs(posCover, plis);
        LOG.info("... done! (" + numFDs + " FDs)");
	}

	private PliCollection calculatePliCollection(TableInput tableInput){
        LOG.info("Calculate plis ...");
        PliCollection plis = readPlis(tableInput);
        plis.sortByNumOfClusters();
        return plis;
    }

	private void calculateFDs(Sampler sampler, Validator validator, Inductor inductor) throws AlgorithmExecutionException {
        List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        do {
            FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
            inductor.updatePositiveCover(newNonFds);
            comparisonSuggestions = validator.validatePositiveCover();
        } while (comparisonSuggestions != null);
    }

    private PliCollection readPlis(TableInput tableInput){
        LOG.info("Reading data");
        PliCollection pliCollection = PliCollection.readFromTableInput(tableInput,
                tableInputProvider.getNumberOfAttributes(), this.valueComparator);
        tableInputProvider.closeInput();
        return pliCollection;
    }

    private FDSet buildNegativeCover(){
        return new FDSet(tableInputProvider.getNumberOfAttributes(), MAX_LHS);
    }
    private FDTree buildPositiveCover(){
        FDTree posCover = new FDTree(tableInputProvider.getNumberOfAttributes(), MAX_LHS);
        posCover.addMostGeneralDependencies();
        return posCover;
    }

    private void informAboutAnyFD(){
        tableInputProvider.buildColumnIdentifiers()
                .stream()
                .map(FunctionalDependency::new)
                .forEach(resultReceiver::receiveResult);
    }

    private int informAboutFDs(FDTree posCover, PliCollection plis){
        return posCover.addFunctionalDependenciesInto(this.resultReceiver,
                tableInputProvider.buildColumnIdentifiers(),
                plis);
    }

	private TableInput initializeTableInput(){
        LOG.info("Initializing ...");
        return tableInputProvider.getInput();
    }

}

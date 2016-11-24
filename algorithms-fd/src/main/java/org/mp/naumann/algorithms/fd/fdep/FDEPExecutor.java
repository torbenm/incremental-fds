package org.mp.naumann.algorithms.fd.fdep;

import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.TableInputProvider;
import org.mp.naumann.algorithms.fd.structures.plis.PliCollection;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;

import java.util.List;
import java.util.logging.Logger;

public class FDEPExecutor implements FunctionalDependencyAlgorithm {

    private final static Logger LOG = Logger.getLogger(FDEPExecutor.class.getName());

	private final TableInputProvider tableInputProvider;
	private final FunctionalDependencyResultReceiver resultReceiver;

	private final ValueComparator valueComparator = new ValueComparator(true);


	public FDEPExecutor(Table table, FunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
		this.tableInputProvider = new TableInputProvider(table);
	}

	public void execute() {
		long startTime = System.currentTimeMillis();
		this.executeFDEP();
		LOG.info("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private void executeFDEP() {
		// Create default output if input is empty
		if (tableInputProvider.getTable().getRowCount() == 0) {
			informAboutAnyFD();
            return;
		}

		PliCollection plis = calculatePliCollection(initializeTableInput());
	    executeFDEP(plis);

        List<FunctionalDependency> result = executeFDEP(plis);
		result.forEach(resultReceiver::receiveResult);
        int numFDs = result.size();
		LOG.info("... done! (" + numFDs + " FDs)");
	}

	private List<FunctionalDependency> executeFDEP(PliCollection plis){
        LOG.info("Executing fdep ...");
        FDEP fdep = new FDEP(tableInputProvider.getNumberOfAttributes(), this.valueComparator);
        FDTree fds = fdep.execute(plis.getCompressed());
        LOG.info("Translating fd-tree into result format ...");
        return fds.getFunctionalDependencies(tableInputProvider.buildColumnIdentifiers(), plis);
    }

    private PliCollection calculatePliCollection(TableInput tableInput){
        LOG.info("Calculate plis ...");
        return readPlis(tableInput);
    }

    private PliCollection readPlis(TableInput tableInput){
        LOG.info("Reading data");
        PliCollection pliCollection = PliCollection.readFromTableInput(tableInput,
                tableInputProvider.getNumberOfAttributes(), this.valueComparator);
        tableInputProvider.closeInput();
        return pliCollection;
    }

    private void informAboutAnyFD(){
        tableInputProvider.buildColumnIdentifiers()
                .stream()
                .map(FunctionalDependency::new)
                .forEach(resultReceiver::receiveResult);
    }

    private TableInput initializeTableInput(){
        LOG.info("Initializing ...");
        return tableInputProvider.getInput();
    }



}

package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.hyfd.HyFD;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.database.Table;

import java.util.ArrayList;
import java.util.List;

public class HyFDInitialAlgorithm implements InitialAlgorithm<List<FunctionalDependency>, FDIntermediateDatastructure> {

	private HyFD hyfd;
	private List<FunctionalDependency> fds = new ArrayList<>();

	public HyFDInitialAlgorithm(IncrementalFDConfiguration configuration, Table table) {
		hyfd = new HyFD(configuration, table, fds::add);
	}
	
	@Override
	public FDIntermediateDatastructure getIntermediateDataStructure() {
		return new FDIntermediateDatastructure(hyfd.getNegCover(), hyfd.getPosCover(), hyfd.getPLIBuilder(), hyfd.getValueComparator(), hyfd.getViolationCollection(), hyfd.getColumns(), hyfd.getPruner());
	}

	@Override
	public List<FunctionalDependency> execute() {
		fds.clear();
		try {
			hyfd.execute();
		} catch (AlgorithmExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fds;
	}

	public List<FunctionalDependency> getFDs() {
		return fds;
	}

}

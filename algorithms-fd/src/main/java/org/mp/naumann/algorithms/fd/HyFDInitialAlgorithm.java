package org.mp.naumann.algorithms.fd;

import java.util.ArrayList;
import java.util.List;

import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.fd.hyfd.HyFD;
import org.mp.naumann.database.Table;

public class HyFDInitialAlgorithm implements InitialAlgorithm<List<FunctionalDependency>, FDIntermediateDatastructure> {

	
	private HyFD hyfd;
	private List<FunctionalDependency> fds = new ArrayList<>();

	public HyFDInitialAlgorithm(Table table) {
		hyfd = new HyFD(table, fds::add);
	}
	
	@Override
	public FDIntermediateDatastructure getIntermediateDataStructure() {
		return new FDIntermediateDatastructure(hyfd.getPosCover(), hyfd.getClusterMaps(), hyfd.getNumRecords(), hyfd.getPliSequence(), hyfd.getFilter());
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

}

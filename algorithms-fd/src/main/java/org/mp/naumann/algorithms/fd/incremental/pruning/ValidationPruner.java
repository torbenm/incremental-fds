package org.mp.naumann.algorithms.fd.incremental.pruning;

import org.mp.naumann.algorithms.fd.structures.LatticeElementLhsPair;

public interface ValidationPruner {

    boolean cannotBeViolated(LatticeElementLhsPair fd);

}

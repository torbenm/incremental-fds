package org.mp.naumann.algorithms;

import org.mp.naumann.algorithms.data.IntermediateDataStructure;
import org.mp.naumann.algorithms.result.ResultSet;

public interface Algorithm {

    ResultSet getResultSet();
    IntermediateDataStructure getIntermediateDataStructure();
}

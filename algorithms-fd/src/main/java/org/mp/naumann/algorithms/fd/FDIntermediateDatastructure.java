package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.DeletePruner;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolationCollection;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

import java.util.List;

public class FDIntermediateDatastructure {

    private final FDTree posCover;
    private final PLIBuilder pliBuilder;
    private final ValueComparator valueComparator;
    private final ViolationCollection violatingValues;
    private final List<String> columns;
    private final DeletePruner pruner;


    public FDIntermediateDatastructure(FDTree posCover, PLIBuilder pliBuilder, ValueComparator valueComparator, ViolationCollection violationCollection, List<String> columns, DeletePruner pruner) {
        this.posCover = posCover;
        this.pliBuilder = pliBuilder;
        this.valueComparator = valueComparator;
        this.violatingValues = violationCollection;
        this.columns = columns;
        this.pruner = pruner;
    }


    public FDTree getPosCover() {
        return posCover;
    }

    public PLIBuilder getPliBuilder() {
        return pliBuilder;
    }

    public ValueComparator getValueComparator() {
        return valueComparator;
    }

    public ViolationCollection getViolatingValues() {
        return violatingValues;
    }

    public List<String> getColumns() {
        return columns;
    }

    public DeletePruner getPruner() {
        return pruner;
    }
}

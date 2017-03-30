package org.mp.naumann.algorithms.fd;

import java.util.List;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.agreesets.AgreeSetCollection;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

public class FDIntermediateDatastructure {

    private final PLIBuilder pliBuilder;
    private final ValueComparator valueComparator;
    private final List<String> columns;
    private final AgreeSetCollection pruner;
    private final List<OpenBitSetFD> functionalDependencies;


    public FDIntermediateDatastructure(List<OpenBitSetFD> functionalDependencies, PLIBuilder pliBuilder,
        ValueComparator valueComparator,
        List<String> columns, AgreeSetCollection pruner) {
        this.pliBuilder = pliBuilder;
        this.valueComparator = valueComparator;
        this.columns = columns;
        this.pruner = pruner;
        this.functionalDependencies = functionalDependencies;
    }

    public PLIBuilder getPliBuilder() {
        return pliBuilder;
    }

    public ValueComparator getValueComparator() {
        return valueComparator;
    }

    public List<String> getColumns() {
        return columns;
    }

    public AgreeSetCollection getPruner() {
        return pruner;
    }

    public List<OpenBitSetFD> getFunctionalDependencies() {
        return functionalDependencies;
    }
}

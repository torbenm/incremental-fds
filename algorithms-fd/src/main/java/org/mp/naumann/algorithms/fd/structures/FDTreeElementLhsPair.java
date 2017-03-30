package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.hyfd.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.database.data.ColumnIdentifier;

public class FDTreeElementLhsPair {

    private final FDTreeElement element;
    private final OpenBitSet lhs;

    public FDTreeElementLhsPair(FDTreeElement element, OpenBitSet lhs) {
        this.element = element;
        this.lhs = lhs;
    }

    public FDTreeElement getElement() {
        return this.element;
    }

    public OpenBitSet getLhs() {
        return this.lhs;
    }

    public Collection<String> toFDStrings() {
        return element.getFdCollection().parallelStream().map(fd -> BitSetUtils.toString(lhs) + " -> " + fd).collect(Collectors.toList());
    }


    public Collection<String> toFDStrings(List<PositionListIndex> plis, ObjectArrayList<ColumnIdentifier> columnIdentifiers) {
        return element
                .getFdCollection(lhs, columnIdentifiers, plis)
                .parallelStream()
                .map(FunctionalDependency::toString)
                .collect(Collectors.toList());
    }
}


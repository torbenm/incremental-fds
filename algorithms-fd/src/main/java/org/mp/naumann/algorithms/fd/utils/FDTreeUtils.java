package org.mp.naumann.algorithms.fd.utils;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.hyfd.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;

public class FDTreeUtils {

    public static List<FDTreeElementLhsPair> getFdLevel(FDTree tree, int level) {
        final List<FDTreeElementLhsPair> currentLevel;
        if (level == 0) {
            currentLevel = new ArrayList<>();
            currentLevel.add(new FDTreeElementLhsPair(tree, new OpenBitSet(tree.getNumAttributes())));
        } else {
            currentLevel = tree.getLevel(level);
        }
        return currentLevel;
    }

    public static List<String> fdLevelToString(FDTree tree, int level) {
        return getFdLevel(tree, level).stream()
                .map(FDTreeElementLhsPair::toFDStrings)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public static List<String> fdLevelToReadableString(FDTree tree, int level, List<PositionListIndex> plis, ObjectArrayList<ColumnIdentifier> columnIdentifiers) {
        return getFdLevel(tree, level).stream()
                .map(f -> f.toFDStrings(plis, columnIdentifiers))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public static String fdToString(OpenBitSet lhs, int rhs, List<PositionListIndex> plis, ObjectArrayList<ColumnIdentifier> columnIdentifiers) {
        ColumnIdentifier[] columns = new ColumnIdentifier[(int) lhs.cardinality()];
        int j = 0;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            int columnId = plis.get(i).getAttribute(); // Here we translate the column i back to the real column i before the sorting
            columns[j++] = columnIdentifiers.get(columnId);
        }

        ColumnCombination colCombination = new ColumnCombination(columns);
        int rhsId = plis.get(rhs).getAttribute(); // Here we translate the column rhs back to the real column rhs before the sorting
        return new FunctionalDependency(colCombination, columnIdentifiers.get(rhsId)).toString();
    }
}

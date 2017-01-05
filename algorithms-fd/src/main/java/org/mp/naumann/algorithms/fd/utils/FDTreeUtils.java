package org.mp.naumann.algorithms.fd.utils;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;

import java.util.ArrayList;
import java.util.List;

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
}

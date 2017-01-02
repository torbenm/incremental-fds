package org.mp.naumann.algorithms.fd.incremental.bloom;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AdvancedBloomPruningStrategyBuilder extends BloomPruningStrategyBuilder {

    private static final int MAX_LEVEL = 13;
    private final FDTree posCover;
    private int maxLevel;

    public AdvancedBloomPruningStrategyBuilder(List<String> columns, FDTree posCover, int maxLevel) {
        super(columns);
        this.posCover = posCover;
        this.maxLevel = maxLevel;
    }

    public AdvancedBloomPruningStrategyBuilder(List<String> columns, FDTree posCover) {
        this(columns, posCover, MAX_LEVEL);
    }

    @Override
    protected Set<OpenBitSet> generateCombinations() {
        Set<OpenBitSet> bloomFds = new HashSet<>();
        int maxLevel = Math.min(posCover.getDepth(), this.maxLevel);
        for (int level = 0; level <= maxLevel; level++) {
            List<FDTreeElementLhsPair> currentLevel = FDTreeUtils.getFdLevel(posCover, level);
            for (FDTreeElementLhsPair fd : currentLevel) {
                bloomFds.add(fd.getLhs());
            }
        }
        return bloomFds;
    }
}

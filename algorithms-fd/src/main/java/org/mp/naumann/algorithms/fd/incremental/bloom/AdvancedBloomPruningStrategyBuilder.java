package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.BloomFilter;

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

    public AdvancedBloomPruningStrategyBuilder(List<String> columns, int numRecords, List<Integer> pliSequence, FDTree posCover) {
        super(columns, numRecords, pliSequence);
        this.posCover = posCover;
    }

    @Override
    protected Set<OpenBitSet> generateCombinations(List<String> cols) {
        Set<OpenBitSet> bloomFds = new HashSet<>();
        int maxLevel = Math.min(cols.size(), MAX_LEVEL);
        for (int level = 0; level <= maxLevel; level++) {
            List<FDTreeElementLhsPair> currentLevel = FDTreeUtils.getFdLevel(posCover, level);
            for (FDTreeElementLhsPair fd : currentLevel) {
                bloomFds.add(fd.getLhs());
            }
        }
        return bloomFds;
    }

    @Override
    protected BloomFilter<Set<ColumnValue>> createFilter() {
        return BloomFilter.create(new ValueCombinationFunnel(), 100_000_000);
    }
}

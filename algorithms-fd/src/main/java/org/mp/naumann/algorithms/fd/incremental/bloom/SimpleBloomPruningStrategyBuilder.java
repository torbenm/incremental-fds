package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.BloomFilter;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.PowerSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SimpleBloomPruningStrategyBuilder extends BloomPruningStrategyBuilder {

    private static final int MAX_LEVEL = 4;

    public SimpleBloomPruningStrategyBuilder(List<String> columns, int numRecords, List<Integer> pliSequence) {
        super(columns, numRecords, pliSequence);
    }

    @Override
    protected Set<OpenBitSet> generateCombinations(List<String> columns) {
        Set<String> columnSet = new HashSet<>(columns);
        int maxLevel = Math.min(columns.size(), MAX_LEVEL);
        return PowerSet.getPowerSet(columnSet, maxLevel).stream().map(cols -> toBitSet(cols, columns.size())).collect(Collectors.toSet());
    }

    @Override
    protected BloomFilter<Set<ColumnValue>> createFilter() {
        return BloomFilter.create(new ValueCombinationFunnel(), 100_000_000);
    }

    private OpenBitSet toBitSet(Collection<String> cols, int numAttributes) {
        OpenBitSet bits = new OpenBitSet(numAttributes);
        for (String column : cols) {
            bits.fastSet(getId(column));
        }
        return bits;
    }
}

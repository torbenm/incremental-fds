package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.BloomFilter;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class AdvancedBloomPruningStrategy extends BloomPruningStrategy {

    private static final int MAX_LEVEL = 13;

    private final Map<Integer, String> idsToColumn = new HashMap<>();
    private final List<List<String>> bloomFds = new ArrayList<>();
    private final FDTree posCover;

    public AdvancedBloomPruningStrategy(List<String> columns, int numRecords, List<Integer> pliSequence, FDTree posCover) {
        super(columns, numRecords, pliSequence, MAX_LEVEL);
        this.posCover = posCover;
        int i = 0;
        for (int id : pliSequence) {
            String column = columns.get(id);
            idsToColumn.put(i++, column);
        }
    }

    @Override
    protected BloomFilter<Set<ColumnValue>> initializeFilter(List<Map<String, String>> invertedRecords) {
        BloomFilter<Set<ColumnValue>> filter = BloomFilter.create(new ValueCombinationFunnel(), 100_000_000);
        for (int level = 0; level <= maxLevel; level++) {
            List<FDTreeElementLhsPair> currentLevel = FDTreeUtils.getFdLevel(posCover, level);
            for (FDTreeElementLhsPair fd : currentLevel) {
                List<String> fdLhs = getColumns(fd);
                bloomFds.add(fdLhs);
                for (Map<String, String> record : invertedRecords) {
                    Set<ColumnValue> combination = getColumnCombination(fdLhs, record);
                    filter.put(combination);
                }
            }
        }
        return filter;
    }

    private Set<ColumnValue> getColumnCombination(List<String> cols, Map<String, String> record) {
        Set<ColumnValue> set = new HashSet<>();
        for (String column : cols) {
            set.add(new ColumnValue(column, record.get(column)));
        }
        return set;
    }

    private List<String> getColumns(FDTreeElementLhsPair fd) {
        int currIndex = 0;
        OpenBitSet lhs = fd.getLhs();
        int next;
        List<String> cols = new ArrayList<>();
        while ((next = lhs.nextSetBit(currIndex)) != -1) {
            cols.add(idsToColumn.get(next));
            currIndex = next + 1;
        }
        return cols;
    }

    @Override
    protected Collection<Set<ColumnValue>> getCombinationsToCheck(InsertStatement insert) {
        List<Set<ColumnValue>> toCheck = new ArrayList<>();
        for (List<String> fdLhs : bloomFds) {
            Set<ColumnValue> combination = getColumnCombination(fdLhs, insert.getValueMap());
            toCheck.add(combination);
        }
        return toCheck;
    }

    @Override
    protected void updateFilter(InsertStatement insert) {
        for (List<String> fdLhs : bloomFds) {
            Set<ColumnValue> combination = getColumnCombination(fdLhs, insert.getValueMap());
            put(combination);
        }
    }

    @Override
    protected Set<Set<ColumnValue>> innerCombinationsToCheck(Batch batch) {
        List<InsertStatement> inserts = batch.getInsertStatements();
        Map<Set<ColumnValue>, Integer> innerCombinations = new HashMap<>();
        for (InsertStatement insert : inserts) {
            for (List<String> fdLhs : bloomFds) {
                Set<ColumnValue> combination = getColumnCombination(fdLhs, insert.getValueMap());
                innerCombinations.merge(combination, 1, Integer::sum);
            }
        }
        return innerCombinations.entrySet().stream()
                .filter(e -> e.getValue() > 1).map(Entry::getKey).collect(Collectors.toSet());
    }
}

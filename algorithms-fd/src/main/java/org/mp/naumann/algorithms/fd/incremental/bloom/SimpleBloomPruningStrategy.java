package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.BloomFilter;

import org.mp.naumann.algorithms.fd.utils.PowerSet;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class SimpleBloomPruningStrategy extends BloomPruningStrategy {

    private static final int MAX_LEVEL = 2;

    public SimpleBloomPruningStrategy(List<String> columns, int numRecords, List<Integer> pliSequence) {
        super(columns, numRecords, pliSequence, MAX_LEVEL);
    }

    @Override
    protected BloomFilter<Set<ColumnValue>> initializeFilter(List<Map<String, String>> invertedRecords) {
        BloomFilter<Set<ColumnValue>> filter = BloomFilter.create(new ValueCombinationFunnel(), 100_000);
        for (int level = 0; level <= maxLevel; level++) {
            for (Map<String, String> record : invertedRecords) {
                ValueCombination vc = new ValueCombination();
                for(Entry<String, String> entry : record.entrySet()) {
                    vc.add(entry.getKey(), entry.getValue());
                }
                for (Set<ColumnValue> combination : vc.getPowerSet(maxLevel)) {
                    filter.put(combination);
                }
            }
        }
        return filter;
    }

    @Override
    protected Collection<Set<ColumnValue>> getCombinationsToCheck(InsertStatement insert) {
        ValueCombination vc = new ValueCombination();
        for (Entry<String, String> entry : insert.getValueMap().entrySet()) {
            vc.add(entry.getKey(), entry.getValue());
        }
        return vc.getPowerSet(maxLevel);
    }

    @Override
    protected void updateFilter(InsertStatement insert) {
        ValueCombination vc = new ValueCombination();
        for (Entry<String, String> entry : insert.getValueMap().entrySet()) {
            vc.add(entry.getKey(), entry.getValue());
        }
        for (Set<ColumnValue> combination : vc.getPowerSet(maxLevel)) {
            put(combination);
        }
    }

    @Override
    protected Set<Set<ColumnValue>> innerCombinationsToCheck(Batch batch) {
        List<InsertStatement> inserts = batch.getInsertStatements();
        Map<Set<ColumnValue>, Integer> innerCombinations = new HashMap<>();
        for (InsertStatement insert : inserts) {
            ValueCombination vc = new ValueCombination();
            for (Entry<String, String> entry : insert.getValueMap().entrySet()) {
                vc.add(entry.getKey(), entry.getValue());
            }
            for (Set<ColumnValue> combination : vc.getPowerSet(maxLevel)) {
                innerCombinations.merge(combination, 1, Integer::sum);
            }
        }
        return innerCombinations.entrySet().stream()
                .filter(e -> e.getValue() > 1).map(Entry::getKey).collect(Collectors.toSet());
    }

    private static class ValueCombination {

        private final Set<ColumnValue> values = new HashSet<>();

        private ValueCombination add(String columnName, String value) {
            values.add(new ColumnValue(columnName, value));
            return this;
        }

        private Set<Set<ColumnValue>> getPowerSet(int maxSize) {
            return PowerSet.getPowerSet(values, maxSize);
        }

    }
}

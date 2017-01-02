package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.BloomFilter;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.CardinalitySet;
import org.mp.naumann.algorithms.fd.incremental.PruningStrategy;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
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
import java.util.logging.Level;
import java.util.stream.Collectors;

public abstract class BloomPruningStrategyBuilder {

    private final List<String> columns;
    private final Map<Integer, String> idsToColumn = new HashMap<>();
    private final Map<String, Integer> columnsToId = new HashMap<>();
    private final List<Integer> pliSequence;
    private final int numRecords;
    private BloomFilter<Set<ColumnValue>> filter;

    private int puts = 0;
    private int requests = 0;
    private Map<OpenBitSet, List<String>> combinations;
    private int bloomViolations = 0;
    private int innerViolations = 0;

    protected BloomPruningStrategyBuilder(List<String> columns, int numRecords, List<Integer> pliSequence) {
        this.columns = columns;
        this.numRecords = numRecords;
        this.pliSequence = pliSequence;
    }

    private List<Map<String, String>> invertRecords(int numRecords, List<HashMap<String, IntArrayList>> clusterMaps) {
        FDLogger.log(Level.FINER, "Inverting records...");
        List<Map<String, String>> invertedRecords = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            invertedRecords.add(new HashMap<>());
        }
        int i = 0;
        for (HashMap<String, IntArrayList> clusterMap : clusterMaps) {
            String column = columns.get(i++);
            for (Entry<String, IntArrayList> entry : clusterMap.entrySet()) {
                String value = entry.getKey();
                for (int id : entry.getValue()) {
                    invertedRecords.get(id).put(column, value);
                }
            }
        }
        FDLogger.log(Level.FINER, "Finished inverting records");
        return invertedRecords;
    }

    protected int getId(String column) {
        return columnsToId.get(column);
    }

    public PruningStrategy buildStrategy(Batch batch) {
        List<InsertStatement> inserts = batch.getInsertStatements();
        int oldRequest = requests;
        int oldBloomViolations = bloomViolations;
        int oldInnerViolations = innerViolations;
        CardinalitySet nonViolations = new CardinalitySet(columns.size());
        for (Entry<OpenBitSet, List<String>> combination : combinations.entrySet()) {
            boolean canBeViolated = false;
            Set<Set<ColumnValue>> inner = new HashSet<>();
            for (InsertStatement insert : inserts) {
                Set<ColumnValue> vc = getValues(insert.getValueMap(), combination.getValue());
                boolean innerViolation = inner.contains(vc);
                boolean bloomViolation = mightContain(vc);
                if (innerViolation) {
                    innerViolations++;
                } else if (bloomViolation) {
                    bloomViolations++;
                }
                if (innerViolation || bloomViolation) {
                    canBeViolated = true;
                    break;
                }
                inner.add(vc);
            }
            if (!canBeViolated) {
                nonViolations.add(combination.getKey());
                FDLogger.log(Level.FINEST, "All combinations new for columns " + combination.getValue());
            }
        }
        int oldPuts = puts;
        for (List<String> fd : combinations.values()) {
            for (InsertStatement insert : inserts) {
                updateFilter(fd, insert.getValueMap());
            }
        }
        FDLogger.log(Level.FINER, "Made " + (requests - oldRequest) + " requests on filter");
        FDLogger.log(Level.FINER, "Made " + (puts - oldPuts) + " puts on filter");
        FDLogger.log(Level.FINER, "Made " + requests + " total requests on filter");
        FDLogger.log(Level.FINER, "Made " + puts + " total puts on filter");
        FDLogger.log(Level.FINER, "Found " + (bloomViolations - oldBloomViolations) + " violations in filter");
        FDLogger.log(Level.FINER, "Found " + (innerViolations - oldInnerViolations) + " inner violations");
        FDLogger.log(Level.FINER, "Found " + bloomViolations + " total violations in filter");
        FDLogger.log(Level.FINER, "Found " + innerViolations + " total inner violations");
        return new BloomPruningStrategy(nonViolations);
    }

    private Set<ColumnValue> getValues(Map<String, String> record, List<String> combination) {
        Set<ColumnValue> set = new HashSet<>();
        for (String column : combination) {
            set.add(new ColumnValue(column, record.get(column)));
        }
        return set;
    }

    private List<String> getColumns(OpenBitSet combination) {
        int currIndex = 0;
        int next;
        List<String> cols = new ArrayList<>();
        while ((next = combination.nextSetBit(currIndex)) != -1) {
            cols.add(idsToColumn.get(next));
            currIndex = next + 1;
        }
        return cols;
    }

    protected abstract Set<OpenBitSet> generateCombinations(List<String> columns);

    private boolean mightContain(Set<ColumnValue> combination) {
        requests++;
        return filter.mightContain(combination);
    }

    public void initialize(List<HashMap<String, IntArrayList>> clusterMaps) {
        int i = 0;
        for (int id : pliSequence) {
            String column = columns.get(id);
            idsToColumn.put(i, column);
            columnsToId.put(column, i);
            i++;
        }
        List<Map<String, String>> invertedRecords = invertRecords(numRecords, clusterMaps);
        combinations = toMap(generateCombinations(columns));
        FDLogger.log(Level.FINER, "Keeping track of " + combinations.size() + " column combinations");
        FDLogger.log(Level.FINER, "Initializing bloom filter...");
        filter = createFilter();
        for (List<String> combination : combinations.values()) {
            for (Map<String, String> record : invertedRecords) {
                updateFilter(combination, record);
            }
        }
        FDLogger.log(Level.FINER, "Finished initializing bloom filter");
    }

    private Map<OpenBitSet, List<String>> toMap(Set<OpenBitSet> fds) {
        return fds.stream().map(bits -> Pair.of(bits, getColumns(bits))).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private void updateFilter(List<String> cols, Map<String, String> record) {
        put(getColumnCombination(cols, record));
    }

    protected abstract BloomFilter<Set<ColumnValue>> createFilter();

    private Set<ColumnValue> getColumnCombination(Collection<String> cols, Map<String, String> record) {
        Set<ColumnValue> set = new HashSet<>();
        for (String column : cols) {
            set.add(new ColumnValue(column, record.get(column)));
        }
        return set;
    }

    private void put(Set<ColumnValue> combination) {
        filter.put(combination);
        puts++;
    }

    private static class BloomPruningStrategy implements PruningStrategy {

        private final CardinalitySet nonViolations;

        private BloomPruningStrategy(CardinalitySet nonViolations) {
            this.nonViolations = nonViolations;
        }

        @Override
        public boolean canBeViolated(FDTreeElementLhsPair fd) {
            OpenBitSet toDo = fd.getLhs().clone();
            for (int level = nonViolations.getDepth(); level >= 0; level--) {
                for (OpenBitSet nonViolation : nonViolations.getLevel(level)) {
                    if (BitSetUtils.isContained(nonViolation, fd.getLhs())) {
                        toDo.andNot(nonViolation);
                        if (toDo.cardinality() == 0) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

    }
}

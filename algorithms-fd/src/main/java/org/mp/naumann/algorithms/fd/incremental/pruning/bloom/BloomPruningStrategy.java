package org.mp.naumann.algorithms.fd.incremental.pruning.bloom;

import com.google.common.hash.BloomFilter;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.pruning.CardinalitySet;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.UpdateStatement;
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

public class BloomPruningStrategy {

    private final List<String> columns;
    private final Collection<BloomGenerator> generators = new ArrayList<>();
    private BloomFilter<Collection<ColumnValue>> filter;
    private int puts = 0;
    private int requests = 0;
    private Map<OpenBitSet, List<Integer>> combinations;
    private int bloomViolations = 0;
    private int innerViolations = 0;

    public BloomPruningStrategy(List<String> columns) {
        this.columns = columns;
    }

    private List<String[]> invertRecords(int numRecords, List<HashMap<String, IntArrayList>> clusterMaps, List<Integer> pliOrder) {
        FDLogger.log(Level.FINER, "Inverting records...");
        List<String[]> invertedRecords = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            invertedRecords.add(new String[columns.size()]);
        }
        int i = 0;
        for (int columnId : pliOrder) {
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(columnId);
            for (Entry<String, IntArrayList> entry : clusterMap.entrySet()) {
                for (int id : entry.getValue()) {
                    invertedRecords.get(id)[i] = entry.getKey();
                }
            }
            i++;
        }
        FDLogger.log(Level.FINER, "Finished inverting records");
        return invertedRecords;
    }

    public BloomPruningStrategy addGenerator(BloomGenerator generator) {
        generators.add(generator);
        return this;
    }

    public ValidationPruner analyzeBatch(Batch batch) {
        List<InsertStatement> inserts = batch.getInsertStatements();
        List<UpdateStatement> updates = batch.getUpdateStatements();
        int oldRequest = requests;
        int oldBloomViolations = bloomViolations;
        int oldInnerViolations = innerViolations;
        CardinalitySet nonViolations = new CardinalitySet(columns.size());
        for (Entry<OpenBitSet, List<Integer>> combination : combinations.entrySet()) {
            boolean isUniqueCombination = isUniqueCombination(inserts, updates, combination.getValue());
            if (isUniqueCombination) {
                nonViolations.add(combination.getKey());
                FDLogger.log(Level.FINEST, "All combinations new for columns " + combination.getValue());
            }
        }
        int oldPuts = puts;
        for (List<Integer> fd : combinations.values()) {
            for (InsertStatement insert : inserts) {
                updateFilter(fd, toArray(insert.getValueMap()));
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
        return new BloomValidationPruner(nonViolations);
    }

    private boolean isUniqueCombination(List<InsertStatement> inserts, List<UpdateStatement> updates, List<Integer> combination) {
        boolean isUniqueCombination = true;
        Set<Collection<ColumnValue>> inner = new HashSet<>();
        List<Map<String, String>> valueMaps = new ArrayList<>(inserts.size() + updates.size());
        valueMaps.addAll(inserts.stream().map(InsertStatement::getValueMap).collect(Collectors.toList()));
        valueMaps.addAll(updates.stream().map(UpdateStatement::getNewValueMap).collect(Collectors.toList()));
        for (Map<String, String> valueMap : valueMaps) {
            Collection<ColumnValue> vc = getValues(toArray(valueMap), combination);
            if (inner.contains(vc)) {
                innerViolations++;
                isUniqueCombination = false;
            } else if (mightContain(vc)) {
                bloomViolations++;
                isUniqueCombination = false;
            }
            if (!isUniqueCombination) {
                break;
            }
            inner.add(vc);
        }
        return isUniqueCombination;
    }

    private String[] toArray(Map<String, String> record) {
        return columns.stream().map(record::get).toArray(String[]::new);
    }

    private Collection<ColumnValue> getValues(String[] record, List<Integer> combination) {
        Collection<ColumnValue> list = new ArrayList<>();
        for (Integer column : combination) {
            list.add(new ColumnValue(column, record[column]));
        }
        return list;
    }

    private boolean mightContain(Collection<ColumnValue> combination) {
        requests++;
        return filter.mightContain(combination);
    }

    public void initialize(List<HashMap<String, IntArrayList>> clusterMaps, int numRecords, List<Integer> pliOrder) {
        Collection<String[]> invertedRecords = invertRecords(numRecords, clusterMaps, pliOrder);
        initialize(invertedRecords);
    }

    public void initialize(Iterable<String[]> invertedRecords) {
        combinations = toMap(generators.stream().flatMap(g -> g.generateCombinations(columns).stream()).collect(Collectors.toSet()));
        int numCombinations = combinations.size();
        FDLogger.log(Level.FINER, "Keeping track of " + numCombinations + " column combinations");
        FDLogger.log(Level.FINER, "Initializing bloom filter...");
        int expectedInsertions = 100_000_000;
        filter = BloomFilter.create(new ValueCombinationFunnel(), expectedInsertions);
        for (List<Integer> combination : combinations.values()) {
            for (String[] record : invertedRecords) {
                updateFilter(combination, record);
            }
        }
        puts = 0;
        FDLogger.log(Level.FINER, "Finished initializing bloom filter");
    }

    private Map<OpenBitSet, List<Integer>> toMap(Set<OpenBitSet> fds) {
        return fds.stream().map(bits -> Pair.of(bits, BitSetUtils.collectSetBits(bits))).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private void updateFilter(List<Integer> cols, String[] record) {
        put(getValues(record, cols));
    }

    private void put(Collection<ColumnValue> combination) {
        filter.put(combination);
        puts++;
    }

    private static class BloomValidationPruner implements ValidationPruner {

        private final CardinalitySet nonViolations;

        private BloomValidationPruner(CardinalitySet nonViolations) {
            this.nonViolations = nonViolations;
        }

        @Override
        public boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet rhs) {
            OpenBitSet canBeViolated = lhs.clone();
            int depth = Math.min(nonViolations.getDepth(), (int) lhs.cardinality());
            for (int level = depth; level >= 0; level--) {
                for (OpenBitSet nonViolation : nonViolations.getLevel(level)) {
                    if (BitSetUtils.isContained(nonViolation, lhs)) {
                        canBeViolated.andNot(nonViolation);
                        if (canBeViolated.isEmpty()) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

    }
}

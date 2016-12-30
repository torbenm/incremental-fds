package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.BloomFilter;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.CardinalitySet;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;

public abstract class BloomPruningStrategy {

    protected final int maxLevel;
    private final List<String> columns;
    private final Map<String, Integer> columnsToId = new HashMap<>();
    private final List<Integer> pliSequence;
    private final int numRecords;
    private BloomFilter<Set<ColumnValue>> filter;
    private int puts = 0;
    private int requests = 0;

    protected BloomPruningStrategy(List<String> columns, int numRecords, List<Integer> pliSequence, int maxLevel) {
        this.columns = columns;
        this.numRecords = numRecords;
        this.pliSequence = pliSequence;
        this.maxLevel = maxLevel;
    }

    private List<Map<String, String>> invertRecords(int numRecords, List<HashMap<String, IntArrayList>> clusterMaps) {
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
        return invertedRecords;
    }

    public CardinalitySet getExistingCombinations(Batch batch) {
        CardinalitySet existingCombinations = new CardinalitySet(maxLevel);
        List<InsertStatement> inserts = batch.getInsertStatements();
        Set<Set<ColumnValue>> innerDoubleCombinations = innerCombinationsToCheck(batch);
        int oldRequest = requests;
        for (InsertStatement insert : inserts) {
            Collection<Set<ColumnValue>> toCheck = getCombinationsToCheck(insert);
            for (Set<ColumnValue> combination : toCheck) {
                if (innerDoubleCombinations.contains(combination) || mightContain(combination)) {
                    OpenBitSet existing = new OpenBitSet(columns.size());
                    for (ColumnValue value : combination) {
                        existing.fastSet(columnsToId.get(value.getColumn()));
                    }
                    existingCombinations.add(existing);
                }
            }
        }
        FDLogger.log(Level.FINER, "Made " + (requests - oldRequest) + " requests on filter");
        int oldPuts = puts;
        for (InsertStatement insert : inserts) {
            updateFilter(insert);
        }
        FDLogger.log(Level.FINER, "Made " + (puts - oldPuts) + " puts on filter");
        FDLogger.log(Level.FINER, "Made " + requests + " total requests on filter");
        FDLogger.log(Level.FINER, "Made " + puts + " total puts on filter");
        return existingCombinations;
    }

    protected boolean mightContain(Set<ColumnValue> combination) {
        requests++;
        return filter.mightContain(combination);
    }

    public void initialize(List<HashMap<String, IntArrayList>> clusterMaps) {
        int i = 0;
        for (int id : pliSequence) {
            String column = columns.get(id);
            columnsToId.put(column, i++);
        }
        List<Map<String, String>> invertedRecords = invertRecords(numRecords, clusterMaps);
        filter = initializeFilter(invertedRecords);
    }

    protected void put(Set<ColumnValue> combination) {
        filter.put(combination);
        puts++;
    }

    protected abstract BloomFilter<Set<ColumnValue>> initializeFilter(List<Map<String, String>> invertedRecords);

    protected abstract Collection<Set<ColumnValue>> getCombinationsToCheck(InsertStatement insert);

    protected abstract void updateFilter(InsertStatement insert);

    protected abstract Set<Set<ColumnValue>> innerCombinationsToCheck(Batch batch);
}

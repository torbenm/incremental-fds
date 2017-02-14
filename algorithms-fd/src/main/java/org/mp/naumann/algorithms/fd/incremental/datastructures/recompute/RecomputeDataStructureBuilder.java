package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import org.mp.naumann.algorithms.benchmark.better.Benchmark;
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RecomputeDataStructureBuilder implements DataStructureBuilder {

    private final RecomputePLIBuilder pliBuilder;
    private final IncrementalFDConfiguration version;
    private final List<String> columns;
    private final Set<Integer> recordIds;

    private List<? extends PositionListIndex> plis;
    private CompressedRecords compressedRecords;

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns) {
        this(pliBuilder, version, columns, pliBuilder.getPliOrder());
    }

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns, List<Integer> pliOrder) {
        this.pliBuilder = new RecomputePLIBuilder(pliBuilder.getClusterMapBuilder(), pliBuilder.isNullEqualNull(), pliOrder, version);
        this.version = version;
        this.columns = columns;
        recordIds = IntStream.range(0, pliBuilder.getNumLastRecords()).boxed().collect(Collectors.toSet());
        updateDataStructures();
    }

    @Override
    public CompressedDiff update(Batch batch) {
        Benchmark benchmark = Benchmark.start("Recompute data structures", Benchmark.DEFAULT_LEVEL + 1);
        Set<Integer> inserted = addRecords(batch.getInsertStatements());
        Set<Integer> insertedUpdate = addUpdateRecords(batch.getUpdateStatements());
        inserted.addAll(insertedUpdate);
        recordIds.addAll(inserted);
        benchmark.finishSubtask("Apply inserts");

        Set<Integer> deleted = removeRecords(batch.getDeleteStatements());
        Set<Integer> deletedUpdate = removeUpdateRecords(batch.getUpdateStatements());
        deleted.addAll(deletedUpdate);
        recordIds.removeAll(deleted);
        benchmark.finishSubtask("Apply deletes");
        Map<Integer, int[]> deletedDiff = new HashMap<>(deleted.size());
        deleted.forEach(i -> deletedDiff.put(i, getCompressedRecord(i)));

        benchmark.startSubtask();
        updateDataStructures(inserted, deleted);
        benchmark.finishSubtask("Update data structures");

        Map<Integer, int[]> insertedDiff = new HashMap<>(inserted.size());
        inserted.forEach(i -> insertedDiff.put(i, getCompressedRecord(i)));

        benchmark.finish();
        return new CompressedDiff(insertedDiff, deletedDiff, new HashMap<>(0), new HashMap<>(0));
    }

    private int[] getCompressedRecord(int record) {
        return version.usesPruningStrategy(PruningStrategy.ANNOTATION) || version.usesPruningStrategy(PruningStrategy.SIMPLE)? compressedRecords.get(record) : null;
    }

    private Set<Integer> removeUpdateRecords(List<UpdateStatement> updates) {
        Set<Integer> ids = new HashSet<>();
        for (UpdateStatement update : updates) {
            Map<String, String> valueMap = update.getOldValueMap();
            Collection<Integer> removed = removeRecord(valueMap);
            ids.addAll(removed);
        }
        return ids;
    }

    private Set<Integer> addUpdateRecords(List<UpdateStatement> updates) {
        Set<Integer> updated = new HashSet<>();
        for (Statement update : updates) {
            Map<String, String> valueMap = update.getValueMap();
            int id = addRecord(valueMap);
            updated.add(id);
        }
        return updated;
    }

    private Set<Integer> removeRecords(List<DeleteStatement> deletes) {
        Set<Integer> ids = new HashSet<>();
        for (Statement delete : deletes) {
            Map<String, String> valueMap = delete.getValueMap();
            Collection<Integer> removed = removeRecord(valueMap);
            ids.addAll(removed);
        }
        return ids;
    }

    private Collection<Integer> removeRecord(Map<String, String> valueMap) {
        List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
        return pliBuilder.removeRecord(values);
    }

    private Set<Integer> addRecords(List<InsertStatement> inserts) {
        Set<Integer> inserted = new HashSet<>();
        for (Statement insert : inserts) {
            Map<String, String> valueMap = insert.getValueMap();
            int id = addRecord(valueMap);
            inserted.add(id);
        }
        return inserted;
    }

    private int addRecord(Map<String, String> valueMap) {
        List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
        return pliBuilder.addRecord(values);
    }

    private void updateDataStructures() {
        plis = pliBuilder.fetchPositionListIndexes();
        RecordCompressor recordCompressor = new ArrayRecordCompressor(recordIds, plis, pliBuilder.getNumRecords());
        compressedRecords = recordCompressor.buildCompressedRecords();
    }

    private void updateDataStructures(Set<Integer> inserted, Set<Integer> deleted) {
        updateDataStructures();

        if (version.usesImprovedSampling() || version.usesInnerClusterPruning()) {
            plis.forEach(pli -> pli.setNewRecords(inserted));
        }

        if (version.usesClusterPruning() || version.usesEnhancedClusterPruning()) {
            Map<Integer, Set<Integer>> newClusters = null;
            if (version.usesEnhancedClusterPruning()) {
                newClusters = new HashMap<>(plis.size());
            }
            for (int i = 0; i < plis.size(); i++) {
                PositionListIndex pli = plis.get(i);
                Set<Integer> clusterIds = extractClustersWithNewRecords(inserted, i);
                if (version.usesClusterPruning()) {
                    pli.setClustersWithNewRecords(clusterIds);
                }
                if (version.usesEnhancedClusterPruning()) {
                    newClusters.put(i, clusterIds);
                }
            }
            if (version.usesEnhancedClusterPruning()) {
                Map<Integer, Set<Integer>> otherClustersWithNewRecords = newClusters;
                plis.forEach(pli -> pli.setOtherClustersWithNewRecords(otherClustersWithNewRecords));
            }
        }

    }

    private Set<Integer> extractClustersWithNewRecords(Collection<Integer> newRecords, int attribute) {
        Set<Integer> clusterIds = new HashSet<>();
        for (int id : newRecords) {
            int clusterId = compressedRecords.get(id)[attribute];
            if (clusterId != PliUtils.UNIQUE_VALUE) {
                clusterIds.add(clusterId);
            }
        }
        return clusterIds;
    }

    public List<? extends PositionListIndex> getPlis() {
        return plis;
    }

    public CompressedRecords getCompressedRecords() {
        return compressedRecords;
    }

    @Override
    public int getNumRecords() {
        return recordIds.size();
    }

}

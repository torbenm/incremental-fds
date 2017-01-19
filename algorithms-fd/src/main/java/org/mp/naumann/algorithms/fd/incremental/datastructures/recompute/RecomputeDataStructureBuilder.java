package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.RecordCompressor;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RecomputeDataStructureBuilder implements DataStructureBuilder {

    private final RecomputePLIBuilder pliBuilder;
    private final IncrementalFDConfiguration version;
    private final List<String> columns;

    private List<? extends PositionListIndex> plis;
    private CompressedRecords compressedRecords;

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns) {
        this(pliBuilder, version, columns, pliBuilder.getPliOrder());
    }

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns, List<Integer> pliOrder) {
        this.pliBuilder = new RecomputePLIBuilder(pliBuilder.getClusterMapBuilder(), pliBuilder.isNullEqualNull(), pliOrder);
        this.version = version;
        this.columns = columns;
    }

    public CompressedDiff update(Batch batch) {

        Set<Integer> inserted = addRecordsToPliBuilder(batch.getInsertStatements());
        Set<Integer> deleted = getRecordIdsToRemove(batch.getDeleteStatements());

        updateDataStructures(inserted, deleted);
        CompressedDiff diff = CompressedDiff.buildDiff(inserted, deleted, version, compressedRecords);
        removeRecords(batch.getDeleteStatements(), deleted);
        updateDataStructures();
        return diff;
    }

    private Set<Integer> getRecordIdsToRemove(List<? extends Statement> stmts){
        Set<Integer> ids = new HashSet<>();
        for (Statement stmt : stmts) {
            Map<String, String> valueMap = stmt.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            ids.addAll(pliBuilder.getMatchingRecordIds(values));
        }
        return ids;
    }

    private Set<Integer> addRecordsToPliBuilder(List<? extends Statement> inserts){
        Set<Integer> inserted = new HashSet<>();
        for (Statement insert : inserts) {
            Map<String, String> valueMap = insert.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            int id = pliBuilder.addRecord(values);
            inserted.add(id);
        }
        return inserted;
    }

    private void removeRecords(List<? extends Statement> stmts, Set<Integer> deleted) {
        for(Statement stmt : stmts){
            Map<String, String> valueMap = stmt.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            pliBuilder.removeRecords(values, deleted);
        }
    }

    private void updateDataStructures() {
        plis = pliBuilder.fetchPositionListIndexes();
        compressedRecords = new ArrayCompressedRecords(RecordCompressor.fetchCompressedRecords(plis, pliBuilder.getNumRecords()));
    }

    private void updateDataStructures(Set<Integer> inserted, Set<Integer> deleted) {
        updateDataStructures();

        if (version.usesInnerClusterPruning()) {
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

        if(version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.ANNOTATION)){
            //Invalidate Entries that are to be removed
            for(int i = 0; i < plis.size(); i++) {
                invalidateRecords(deleted, i);
            }
        }

    }

    private void invalidateRecords(Collection<Integer> oldRecords, int attribute){
        for(int id : oldRecords) {
            int clusterId = compressedRecords.get(id)[attribute];
            if(clusterId > -1) {
                IntArrayList cluster =  plis.get(attribute).getCluster(clusterId);
                cluster.remove((Integer) id);
                if(cluster.size() == 0) {
                    plis.get(attribute).setCluster(clusterId, null);
                }
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

    public CompressedRecords getCompressedRecord() {
        return compressedRecords;
    }

    private static class ArrayCompressedRecords implements CompressedRecords {
        private final int[][] compressedRecords;

        private ArrayCompressedRecords(int[][] compressedRecords) {
            this.compressedRecords = compressedRecords;
        }

        @Override
        public int[] get(int index) {
            return compressedRecords[index];
        }

        @Override
        public void fill(int index, int value) {
            Arrays.fill(compressedRecords[index], value);
        }

        @Override
        public int size() {
            return compressedRecords.length;
        }
    }
}

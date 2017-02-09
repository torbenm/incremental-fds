package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.MapCompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
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
        this.pliBuilder = new RecomputePLIBuilder(pliBuilder.getClusterMapBuilder(), pliBuilder.isNullEqualNull(), pliOrder);
        this.version = version;
        this.columns = columns;
        recordIds = IntStream.range(0, pliBuilder.getNumLastRecords()).boxed().collect(Collectors.toSet());
        updateDataStructures();
    }

    public CompressedDiff update(Batch batch) {
        Set<Integer> inserted = addRecords(batch.getInsertStatements());
        recordIds.addAll(inserted);
        Set<Integer> deleted = removeRecords(batch.getDeleteStatements());
        recordIds.removeAll(deleted);
        Map<Integer, int[]> deletedDiff = new HashMap<>(deleted.size());

        if (version.usesPruningStrategy(PruningStrategy.ANNOTATION)) {
            for (int delete : deleted) {
                deletedDiff.put(delete, compressedRecords.get(delete));
            }
        }

        updateDataStructures(inserted, deleted);
        Map<Integer, int[]> insertedDiff = new HashMap<>(inserted.size());
        if (version.usesPruningStrategy(PruningStrategy.SIMPLE)) {
            for (int insert : inserted) {
                insertedDiff.put(insert, compressedRecords.get(insert));
            }
        }
        CompressedDiff diff = new CompressedDiff(insertedDiff, deletedDiff, new HashMap<>(0), new HashMap<>(0));
        return diff;
    }

    private Set<Integer> removeRecords(List<? extends Statement> deletes){
        Set<Integer> ids = new HashSet<>();
        for (Statement delete : deletes) {
            Map<String, String> valueMap = delete.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            ids.addAll(pliBuilder.removeRecord(values));
       }
        return ids;
    }

    private Set<Integer> addRecords(List<InsertStatement> inserts){
        Set<Integer> inserted = new HashSet<>();
        for (Statement insert : inserts) {
            Map<String, String> valueMap = insert.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            int id = pliBuilder.addRecord(values);
            inserted.add(id);
        }
        return inserted;
    }

    private void updateDataStructures() {
        plis = pliBuilder.fetchPositionListIndexes();
        compressedRecords = buildCompressedRecords();
    }

    private List<Map<Integer, Integer>> invertPlis() {
        List<Map<Integer, Integer>> invertedPlis = new ArrayList<>();
        for (PositionListIndex pli : plis) {
            Map<Integer, Integer> invertedPli = new HashMap<>();

            int clusterId = 0;
            for (IntArrayList cluster : pli.getClusters()) {
                for (int recordId : cluster) {
                    invertedPli.put(recordId, clusterId);
                }
                clusterId++;
            }
            invertedPlis.add(invertedPli);
        }
        return invertedPlis;
    }
/*
    private void removeRecords(List<? extends Map<String, Collection<Integer>>> removalMap){
        pliBuilder.removeRecords(removalMap);
    }*/

    private CompressedRecords buildCompressedRecords() {
        MapCompressedRecords compressedRecords = new MapCompressedRecords();
        List<Map<Integer, Integer>> invertedPlis = invertPlis();
        for (int recordId : recordIds) {
            compressedRecords.put(recordId, fetchRecordFrom(recordId, invertedPlis));
        }
        return compressedRecords;
    }

    private static int[] fetchRecordFrom(int recordId, List<Map<Integer, Integer>> invertedPlis) {
        int numAttributes = invertedPlis.size();
        int[] record = new int[numAttributes];
        for (int i = 0; i < numAttributes; i++) {
            record[i] = invertedPlis.get(i).getOrDefault(recordId, PliUtils.UNIQUE_VALUE);
        }
        return record;
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
/*
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
                /*
                Do we need this? THis should release some space
                if(cluster.size() == 0) {
                    plis.get(attribute).setCluster(clusterId, null);
                }
            }
        }
*/

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
}

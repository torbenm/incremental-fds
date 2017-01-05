package org.mp.naumann.algorithms.fd.incremental;

import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.RecordCompressor;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class IncrementalPLIBuilder {

    private final PLIBuilder pliBuilder;
    private final IncrementalFDVersion version;
    private final List<String> columns;

    private List<PositionListIndex> plis;
    private int[][] compressedRecords;

    public IncrementalPLIBuilder(PLIBuilder pliBuilder, IncrementalFDVersion version, List<String> columns) {
        this.pliBuilder = pliBuilder;
        this.version = version;
        this.columns = columns;
    }

    public CompressedDiff update(Batch batch) {
        Set<Integer> inserted = addRecordsToClusterMap(batch.getInsertStatements());
        Set<Integer> deleted = removeRecordsFromClusterMap(batch.getDeleteStatements());
        updateDataStructures(inserted, deleted);
        return buildDiff(inserted, deleted);
    }

    private Set<Integer> addRecordsToClusterMap(List<? extends Statement> stmts){
        Set<Integer> ids = new HashSet<>();
        for (Statement stmt : stmts) {
            Map<String, String> valueMap = stmt.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            int id = pliBuilder.addRecord(values);
            ids.add(id);
        }
        return ids;
    }

    private Set<Integer> removeRecordsFromClusterMap(List<? extends Statement> stmts){
        Set<Integer> ids = new HashSet<>();
        for (Statement stmt : stmts) {
            Map<String, String> valueMap = stmt.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            ids.addAll(pliBuilder.removeRecord(values));
        }
        return ids;
    }

    private CompressedDiff buildDiff(Collection<Integer> inserted, Collection<Integer> deleted) {
        int[][] insertedRecords = diffToArray(inserted, this.version.getInsertPruningStrategy() == IncrementalFDVersion.InsertPruningStrategy.SIMPLE);
        int[][] deletedRecords = diffToArray(deleted, this.version.getDeletePruningStrategy() == IncrementalFDVersion.DeletePruningStrategy.ANNOTATION);

        int[][] oldUpdatedRecords = new int[0][];
        int[][] newUpdatedRecords = new int[0][];
        return new CompressedDiff(insertedRecords, deletedRecords, oldUpdatedRecords, newUpdatedRecords);
    }

    private int[][] diffToArray(Collection<Integer> diff, boolean doBuild){
        int[][] array = new int[diff.size()][];
        if (doBuild) {
            int i = 0;
            for (int id : diff) {
                array[i] = compressedRecords[id];
                i++;
            }
        }
        return array;
    }

    private void updateDataStructures(Set<Integer> inserted, Set<Integer> deleted) {
        plis = pliBuilder.fetchPositionListIndexes();
        compressedRecords = RecordCompressor.fetchCompressedRecords(plis, pliBuilder.getNumLastRecords());
        if (version.useClusterPruning()) {
            for(int i = 0; i < plis.size(); i++) {
                PositionListIndex pli = plis.get(i);
                pli.setClustersWithNewRecords(getClustersWithNewRecords(inserted, i));
            }
        }
        if(version.getDeletePruningStrategy() == IncrementalFDVersion.DeletePruningStrategy.ANNOTATION){
            //Invalidate Entries that are to be removed
            for(int i = 0; i < plis.size(); i++) {
                invalidateRecords(deleted, i);
            }
        }

    }
    private void invalidateRecords(Collection<Integer> oldRecords, int attribute){
        for(int id : oldRecords) {
            int clusterId = compressedRecords[id][attribute];
            plis.get(attribute).getClusters().get(clusterId).remove((Integer)id);
        }
    }
    private Set<Integer> getClustersWithNewRecords(Collection<Integer> newRecords, int attribute) {
        Set<Integer> clusterIds = new HashSet<>();
        for(int id : newRecords) {
            int clusterId = compressedRecords[id][attribute];
            if (clusterId != -1) {
                clusterIds.add(clusterId);
            }
        }
        return clusterIds;
    }

    public List<PositionListIndex> getPlis() {
        return plis;
    }

    public int[][] getCompressedRecord() {
        return compressedRecords;
    }
}

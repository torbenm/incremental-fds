package org.mp.naumann.algorithms.fd.incremental;

import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.RecordCompressor;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class IncrementalPLIBuilder {

    private final PLIBuilder pliBuilder;
    private final IncrementalFDConfiguration version;
    private final List<String> columns;

    private List<PositionListIndex> plis;
    private int[][] compressedRecords;

    public IncrementalPLIBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns) {
        this.pliBuilder = pliBuilder;
        this.version = version;
        this.columns = columns;
    }

    public CompressedDiff update(Batch batch) {
        Set<Integer> inserted = new HashSet<>();
        List<InsertStatement> inserts = batch.getInsertStatements();
        for (InsertStatement insert : inserts) {
            Map<String, String> valueMap = insert.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            int id = pliBuilder.addRecord(values);
            inserted.add(id);
        }
        updateDataStructures(inserted);
        return buildDiff(inserted);
    }

    private CompressedDiff buildDiff(Collection<Integer> inserted) {
        int[][] insertedRecords = new int[inserted.size()][];
        if (version.getPruningStrategies().contains(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
            int i = 0;
            for (int id : inserted) {
                insertedRecords[i] = compressedRecords[id];
                i++;
            }
        }
        int[][] deletedRecords = new int[0][];
        int[][] oldUpdatedRecords = new int[0][];
        int[][] newUpdatedRecords = new int[0][];
        return new CompressedDiff(insertedRecords, deletedRecords, oldUpdatedRecords, newUpdatedRecords);
    }

    private void updateDataStructures(Set<Integer> inserted) {
        plis = pliBuilder.fetchPositionListIndexes();
        compressedRecords = RecordCompressor.fetchCompressedRecords(plis, pliBuilder.getNumLastRecords());
        if (version.usesClusterPruning()) {
            for(int i = 0; i < plis.size(); i++) {
                PositionListIndex pli = plis.get(i);
                pli.setClustersWithNewRecords(getClustersWithNewRecords(inserted, i));
            }
        }
        if(version.usesInnerClusterPruning()) {
            plis.forEach(pli -> pli.setNewRecords(inserted));
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

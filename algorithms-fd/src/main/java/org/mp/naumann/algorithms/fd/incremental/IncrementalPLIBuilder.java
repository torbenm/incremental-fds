package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.database.statement.InsertStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by dennis on 24.11.16.
 */
public class IncrementalPLIBuilder {
    private int numRecords;
    private List<Set<String>> columnValues;
    private List<HashMap<String, IntArrayList>> clusterMaps;
    private int numAttributes;
    private List<PositionListIndex> plis;
    private int[][] invertedPlis;

    public IncrementalPLIBuilder(int numRecords, List<Set<String>> columnValues, List<HashMap<String, IntArrayList>> clusterMaps, int numAttributes) {
        this.numRecords = numRecords;
        this.columnValues = columnValues;
        this.clusterMaps = clusterMaps;
        this.numAttributes = numAttributes;
    }

    public void updateClusterMapsWithInserts(List<InsertStatement> inserts, List<String> columns) {
        for (InsertStatement insert : inserts) {
            for (String column : columns) {
                Map<String, String> valueMap = insert.getValueMap();
                clusterMaps.get(columns.indexOf(column)).get(valueMap.get(column)).add(++numRecords);
            }
        }
    }

    // the isNullEqualNull parameter does not (yet?) serve a purpose here
    public List<PositionListIndex> recalculatePositionListIndexes(boolean isNullEqualNull) {
        plis = new ArrayList<>();
        for (int columnId = 0; columnId < clusterMaps.size(); columnId++) {
            List<IntArrayList> clusters = new ArrayList<>();
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(columnId);

            if (!isNullEqualNull)
                clusterMap.remove(null);

            for (IntArrayList cluster : clusterMap.values())
                if (cluster.size() > 1)
                    clusters.add(cluster);

            plis.add(new PositionListIndex(columnId, clusters));
        }
        return plis;
    }

    public int[][] recalculateCompressedRecords() {
        invertPlis();
        int[][] compressedRecords = new int[numRecords][];
        for (int recordId = 0; recordId < numRecords; recordId++) {
            compressedRecords[recordId] = this.fetchRecordFrom(recordId);
        }
        return compressedRecords;
    }

    private void invertPlis() {
        invertedPlis = new int[plis.size()][];
        for (int attr = 0; attr < plis.size(); attr++) {
            int[] invertedPli = new int[numRecords];
            Arrays.fill(invertedPli, -1);

            for (int clusterId = 0; clusterId < plis.get(attr).size(); clusterId++) {
                for (int recordId : plis.get(attr).getClusters().get(clusterId))
                    invertedPli[recordId] = clusterId;
            }
            invertedPlis[attr] = invertedPli;
        }
    }

    private int[] fetchRecordFrom(int recordId) {
        int[] record = new int[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++)
            record[i] = invertedPlis[i][recordId];
        return record;
    }

    public List<HashMap<String, IntArrayList>> getClusterMaps() {
        return clusterMaps;
    }

    public void setClusterMaps(List<HashMap<String, IntArrayList>> clusterMaps) {
        this.clusterMaps = clusterMaps;
    }
}

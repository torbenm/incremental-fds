package org.mp.naumann.algorithms.fd.incremental;

import com.google.common.hash.BloomFilter;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IncrementalPLIBuilder {

    private int numRecords;
    private IncrementalFDVersion version;
	private List<HashMap<String, IntArrayList>> clusterMaps;
	private List<PositionListIndex> plis;
	private final List<String> columns;
	private final List<Integer> pliSequence;
	private int[][] compressedRecords;

	public IncrementalPLIBuilder(IncrementalFDVersion version, int numRecords, List<HashMap<String, IntArrayList>> clusterMaps, List<String> columns, List<Integer> pliSequence) {
		this.numRecords = numRecords;
		this.clusterMaps = clusterMaps;
		this.columns = columns;
		this.pliSequence = pliSequence;
        this.version = version;
		updateDataStructures();
	}

	public CompressedDiff update(Batch batch) {
		List<InsertStatement> inserts = batch.getInsertStatements();
		List<Integer> insertedIds = new ArrayList<>();
		for (InsertStatement insert : inserts) {
			int i = 0;
			for (String column : columns) {
				Map<String, String> valueMap = insert.getValueMap();
				HashMap<String, IntArrayList> clusterMap = clusterMaps.get(i);
				String value = valueMap.get(column);
				IntArrayList cluster = clusterMap.get(value);
				if (cluster == null) {
					cluster = new IntArrayList();
					clusterMap.put(value, cluster);
				}
				cluster.add(numRecords);
				i++;
			}
			insertedIds.add(numRecords);
			numRecords++;
		}
		updateDataStructures();
		return buildDiff(insertedIds);
	}

	private CompressedDiff buildDiff(List<Integer> insertedIds) {
		int[][] insertedRecords = new int[insertedIds.size()][];
		int i = 0;
        if(this.version.getPruningStrategy() == IncrementalFDVersion.PruningStrategy.SIMPLE){
            for(int id : insertedIds) {
                insertedRecords[i] = compressedRecords[id];
                i++;
            }
        }
        int[][] deletedRecords = new int[0][];
        int[][] oldUpdatedRecords = new int[0][];
        int[][] newUpdatedRecords = new int[0][];
        return new CompressedDiff(insertedRecords, deletedRecords, oldUpdatedRecords, newUpdatedRecords);
    }

    private void updateDataStructures() {
        plis = recalculatePositionListIndexes(true);
        compressedRecords = recalculateCompressedRecords();
    }

    private List<PositionListIndex> recalculatePositionListIndexes(boolean isNullEqualNull) {
        List<PositionListIndex> clustersPerAttribute = new ArrayList<>();
        for (int columnId : pliSequence) {
            List<IntArrayList> clusters = new ArrayList<>();
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(columnId);

            if (!isNullEqualNull)
                clusterMap.remove(null);

            for (IntArrayList cluster : clusterMap.values())
                if (cluster.size() > 1)
                    clusters.add(cluster);

            clustersPerAttribute.add(new PositionListIndex(columnId, clusters));
        }
        return clustersPerAttribute;
    }

    public int[][] recalculateCompressedRecords() {
        int[][] invertedPlis = PliUtils.invert(plis, numRecords);

        // Extract the integer representations of all records from the inverted
        // plis
        int[][] compressedRecords = new int[numRecords][];
        for (int recordId = 0; recordId < numRecords; recordId++)
            compressedRecords[recordId] = this.fetchRecordFrom(recordId, invertedPlis);
        invertedPlis = null;
        return compressedRecords;
    }

    private int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
        int[] record = new int[columns.size()];
        for (int i = 0; i < record.length; i++)
            record[i] = invertedPlis[i][recordId];
        return record;
    }

    public List<PositionListIndex> getPlis() {
        return plis;
    }

    public int[][] getCompressedRecord() {
        return compressedRecords;
    }
}

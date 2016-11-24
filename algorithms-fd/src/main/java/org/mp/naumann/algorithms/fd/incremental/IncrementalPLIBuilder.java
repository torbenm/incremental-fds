package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by dennis on 24.11.16.
 */
public class IncrementalPLIBuilder {

	private int numRecords;
	private List<HashMap<String, IntArrayList>> clusterMaps;
	private List<PositionListIndex> plis;
	private final List<String> columns;
	private int[][] compressedRecords;

	public IncrementalPLIBuilder(int numRecords, List<HashMap<String, IntArrayList>> clusterMaps, List<String> columns) {
		this.numRecords = numRecords;
		this.clusterMaps = clusterMaps;
		this.columns = columns;
		updateDataStructures();
	}

	public void update(Batch batch) {
		List<InsertStatement> inserts = batch.getInsertStatements();
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
			numRecords++;
		}
		updateDataStructures();
	}

	private void updateDataStructures() {
		plis = recalculatePositionListIndexes(true);
		sortPlis();
		compressedRecords = recalculateCompressedRecords();
	}

	public void sortPlis() {
		Collections.sort(plis, new Comparator<PositionListIndex>() {

			@Override
			public int compare(PositionListIndex o1, PositionListIndex o2) {
				int numClustersInO1 = numRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
				int numClustersInO2 = numRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
				return numClustersInO2 - numClustersInO1;
			}
		});
	}

	private List<PositionListIndex> recalculatePositionListIndexes(boolean isNullEqualNull) {
		List<PositionListIndex> clustersPerAttribute = new ArrayList<>();
        for (int columnId = 0; columnId < clusterMaps.size(); columnId++) {
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

	public List<Set<String>> getValueSets() {
		return clusterMaps.stream().map(Map::keySet).collect(Collectors.toList());
	}

	public List<PositionListIndex> getPlis() {
		return plis;
	}
	
	public int[][] getCompressedRecord() {
		return compressedRecords;
	}
}

/*
 * Copyright 2014 by the Metanome project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PLIBuilder {
	
	private int numRecords = 0;
	
	public int getNumLastRecords() {
		return this.numRecords;
	}
	
	public List<PositionListIndex> getPLIs(TableInput tableInput, int numAttributes, boolean isNullEqualNull) {
		List<HashMap<String, IntArrayList>> clusterMaps = this.calculateClusterMaps(tableInput, numAttributes);
		return this.fetchPositionListIndexes(clusterMaps, isNullEqualNull);
	}

	private List<HashMap<String, IntArrayList>> calculateClusterMaps(TableInput tableInput, int numAttributes) {
		List<HashMap<String, IntArrayList>> clusterMaps = new ArrayList<>();
		for (int i = 0; i < numAttributes; i++)
			clusterMaps.add(new HashMap<>());
		
		this.numRecords = 0;
		while (tableInput.hasNext()) {
			Row record = tableInput.next();
			
			int attributeId = 0;
			for (String value : record) {
				HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
				
				if (clusterMap.containsKey(value)) {
					clusterMap.get(value).add(this.numRecords);
				}
				else {
					IntArrayList newCluster = new IntArrayList();
					newCluster.add(this.numRecords);
					clusterMap.put(value, newCluster);
				}
				
				attributeId++;
			}
			this.numRecords++;
			if (this.numRecords == Integer.MAX_VALUE - 1)
				throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + this.numRecords + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);
		}
		
		return clusterMaps;
	}
	
	private List<PositionListIndex> fetchPositionListIndexes(List<HashMap<String, IntArrayList>> clusterMaps,
			boolean isNullEqualNull) {

		return fetchPositionListIndexesStatic(clusterMaps, isNullEqualNull);

	}
	
	public static List<PositionListIndex> getPLIs(ObjectArrayList<Row> records, int numAttributes, boolean isNullEqualNull) {
		if (records.size() > Integer.MAX_VALUE)
			throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + records.size() + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);
		
		List<HashMap<String, IntArrayList>> clusterMaps = calculateClusterMapsStatic(records, numAttributes);
		return fetchPositionListIndexesStatic(clusterMaps, isNullEqualNull);
	}

	private static List<HashMap<String, IntArrayList>> calculateClusterMapsStatic(ObjectArrayList<Row> records,
			int numAttributes) {
		List<HashMap<String, IntArrayList>> clusterMaps = new ArrayList<>();
		for (int i = 0; i < numAttributes; i++)
			clusterMaps.add(new HashMap<>());
		
		int recordId = 0;
		for (Row record : records) {
			int attributeId = 0;
			for (String value : record) {
				HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
				
				if (clusterMap.containsKey(value)) {
					clusterMap.get(value).add(recordId);
				}
				else {
					IntArrayList newCluster = new IntArrayList();
					newCluster.add(recordId);
					clusterMap.put(value, newCluster);
				}
				
				attributeId++;
			}
			recordId++;
		}
		
		return clusterMaps;
	}
	
	private static List<PositionListIndex> fetchPositionListIndexesStatic(
			List<HashMap<String, IntArrayList>> clusterMaps, boolean isNullEqualNull) {

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
}

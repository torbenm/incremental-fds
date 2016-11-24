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

package org.mp.naumann.algorithms.fd.structures.plis;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PliBuilder {
	
	private int numRecords = 0;
	
	public int getNumLastRecords() {
		return this.numRecords;
	}
	
	public List<PositionListIndex> getPlis(TableInput tableInput, int numAttributes, boolean isNullEqualNull) {
		List<HashMap<String, IntArrayList>> clusterMaps = this.calculateClusterMaps(tableInput, numAttributes);
		return this.fetchPositionListIndexes(clusterMaps, isNullEqualNull);
	}

	/**
	 * Calculates the clusterMap for each column of a given relation.
	 * A clusterMap is a mapping of attributes (by position in the relation, e.g. 0 - n)
	 * to attribute values to record identifiers.
	 *
	 * @param tableInput the table/relation used as input, e.g. from a DB
	 * @param numAttributes the number of attributes in the given relation
	 * @return the list of clusterMaps, as described above
	 */
	private List<HashMap<String, IntArrayList>> calculateClusterMaps(TableInput tableInput, int numAttributes) {
		List<HashMap<String, IntArrayList>> clusterMaps = new ArrayList<>();
		for (int i = 0; i < numAttributes; i++)
			clusterMaps.add(new HashMap<>());
		
		numRecords = 0;
		while (tableInput.hasNext()) {
			Row record = tableInput.next();
			
			int attributeId = 0;
			for (String value : record) {
				HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
				
				if (clusterMap.containsKey(value)) {
					clusterMap.get(value).add(numRecords);
				}
				else {
					IntArrayList newCluster = new IntArrayList();
					newCluster.add(numRecords);
					clusterMap.put(value, newCluster);
				}
				
				attributeId++;
			}
			numRecords++;
			if (numRecords == Integer.MAX_VALUE - 1)
				throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + this.numRecords + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);
		}
		
		return clusterMaps;
	}
	
	private List<PositionListIndex> fetchPositionListIndexes(List<HashMap<String, IntArrayList>> clusterMaps,
			boolean isNullEqualNull) {
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

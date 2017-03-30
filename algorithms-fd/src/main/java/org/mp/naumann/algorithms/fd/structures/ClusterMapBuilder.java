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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

public class ClusterMapBuilder {

    private final List<HashMap<String, IntArrayList>> clusterMaps;
    private int numRecords = 0;


    public ClusterMapBuilder(int numAttributes) {
        clusterMaps = new ArrayList<>();
        for (int i = 0; i < numAttributes; i++) {
            clusterMaps.add(new HashMap<>());
        }
    }

    public List<HashMap<String, IntArrayList>> getClusterMaps() {
        return clusterMaps;
    }

    public int getNumLastRecords() {
        return this.numRecords;
    }

    /**
     * Calculates the clusterMap for each column of a given relation.
     * A clusterMap is a mapping of attributes (by position in the relation, e.g. 0 - n)
     * to attribute values to record identifiers.
     *
     * @param tableInput the table/relation used as input, e.g. from a DB
     */
    void addRecords(TableInput tableInput) {
        while (tableInput.hasNext()) {
            Row record = tableInput.next();

            addRecord(record);
            if (this.numRecords == Integer.MAX_VALUE - 1)
                throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + this.numRecords + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);
        }
    }

    private void addRecord(Iterable<String> record) {
        int recId = this.numRecords;
        int attributeId = 0;

        for (String value : record) {
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
            if (clusterMap.containsKey(value)) {
                clusterMap.get(value).add(recId);
            } else {
                IntArrayList newCluster = new IntArrayList();
                newCluster.add(recId);
                clusterMap.put(value, newCluster);
            }

            attributeId++;
        }
        this.numRecords++;
    }


    void addRecords(Collection<? extends Iterable<String>> records) {
        if (records.size() > Integer.MAX_VALUE)
            throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + records.size() + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);

        for (Iterable<String> record : records) {
            addRecord(record);
        }
    }

}

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
import it.unimi.dsi.fastutil.ints.IntArraySet;

import org.mp.naumann.algorithms.fd.utils.CollectionUtils;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class ClusterMapBuilder {

    private int numRecords = 0;
    private final List<HashMap<String, IntArrayList>> clusterMaps;
    private final HashMap<Long, IntArrayList> hashedRecords;


    public List<HashMap<String, IntArrayList>> getClusterMaps() {
        return clusterMaps;
    }

    public int getNumLastRecords() {
        return this.numRecords;
    }

    public ClusterMapBuilder(int numAttributes) {
        clusterMaps = new ArrayList<>();
        for (int i = 0; i < numAttributes; i++) {
            clusterMaps.add(new HashMap<>());
        }
        hashedRecords = new HashMap<>();
    }


    /**
     * Calculates the clusterMap for each column of a given relation.
     * A clusterMap is a mapping of attributes (by position in the relation, e.g. 0 - n)
     * to attribute values to record identifiers.
     *
     * @param tableInput the table/relation used as input, e.g. from a DB
     * @return the list of clusterMaps, as described above
     */
    public void addRecords(TableInput tableInput) {
        while (tableInput.hasNext()) {
            Row record = tableInput.next();

            addRecord(record);
            if (this.numRecords == Integer.MAX_VALUE - 1)
                throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + this.numRecords + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);
        }
    }

    public int addRecord(Iterable<String> record) {
        int recId = this.numRecords;
        int attributeId = 0;
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();

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
        addRecordToHashMap(hashCodeBuilder.build(), recId);
        return recId;
    }


    public void addRecords(Collection<? extends Iterable<String>> records) {
        if (records.size() > Integer.MAX_VALUE)
            throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + records.size() + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);

        for (Iterable<String> record : records) {
            addRecord(record);
        }
    }


    private void addRecordToHashMap(long hashcode, int recId) {
        if (!hashedRecords.containsKey(hashcode))
            hashedRecords.put(hashcode, new IntArrayList());
        hashedRecords.get(hashcode).add(recId);
    }

    public Collection<Integer> removeRecordWithHashMap(Iterable<String> record) {
        Collection<Integer> records = new IntArraySet(); // new IntArrayList();
        HashCodeBuilder builder = new HashCodeBuilder();
        record.forEach(builder::append);
        IntArrayList recId = hashedRecords.get(builder.build());
        if (recId != null) {
            records.addAll(recId);
            hashedRecords.remove(builder.build());
        }
        return records;
    }


    public Collection<Integer> removeRecord(Iterable<String> record) {
        int attributeId = 0;
        List<Collection<Integer>> clusters = new ArrayList<>();
        for (String value : record) {
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
            IntArrayList cluster = clusterMap.get(value);
            if (cluster == null || cluster.isEmpty()) {
                return Collections.emptyList();
            }
            clusters.add(cluster);
            attributeId++;
        }
        Set<Integer> matching = CollectionUtils.intersection(clusters);
        clusters.forEach(c -> c.removeAll(matching));
        return matching;
    }

}

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

import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterMapBuilder {
    private int numRecords = 0;
    private final List<HashMap<Integer, IntArrayList>> clusterMaps;
    private final Dictionary<String> dictionary = new Dictionary<>();
    private final HashMap<Long, Integer> hashedRecords;

    public List<HashMap<Integer, IntArrayList>> getClusterMaps() {
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
            hashCodeBuilder.append(value);
            int dictValue = dictionary.getOrAdd(value);
            HashMap<Integer, IntArrayList> clusterMap = clusterMaps.get(attributeId);
            if (clusterMap.containsKey(dictValue)) {
                clusterMap.get(dictValue).add(recId);
            } else {
                IntArrayList newCluster = new IntArrayList();
                newCluster.add(recId);
                clusterMap.put(dictValue, newCluster);
            }

            attributeId++;
        }
        //TODO: Handling of duplicates
        // if(this.hashedRecords.containsKey(hashCodeBuilder.build()))
        this.hashedRecords.put(hashCodeBuilder.build(), recId);
        this.numRecords++;
        return recId;
    }

    public Collection<Integer> getMatchingRecordsByHashMap(Iterable<String> record) {
        Collection<Integer> records = new IntArraySet(); // new IntArrayList();
        HashCodeBuilder builder = new HashCodeBuilder();
        record.forEach(builder::append);
        int recId = hashedRecords.getOrDefault(builder.build(), -1);
        if (recId > -1)
            records.add(recId);
        return records;
    }

    public Collection<Integer> getMatchingRecordsByHashMapAndCreateRemovalMap(Iterable<String> record,
                                                                              List<? extends Map<String, Collection<Integer>>> removalMap) {
        Collection<Integer> records = new IntArraySet(); // new IntArrayList();
        HashCodeBuilder builder = new HashCodeBuilder();
        record.forEach(builder::append);
        int recId = hashedRecords.getOrDefault(builder.build(), -1);
        if (recId > -1)
            records.add(recId);
        fillRemovalMap(record, records, removalMap);
        return records;
    }

    private void fillRemovalMap(Iterable<String> record,
                                Collection<Integer> recordIds,
                                List<? extends Map<String, Collection<Integer>>> removalMap) {
        int attributeId = 0;
        for (String v : record) {
            Map<String, Collection<Integer>> map = removalMap.get(attributeId);
            if (!map.containsKey(v)) {
                map.put(v, recordIds);
            } else {
                map.get(v).addAll(recordIds);
            }
        }
    }
/*
    public Collection<Integer> getMatchingRecordsByClusterMaps(Iterable<String> record) {
        int attributeId = 0;
        List<IntArrayList> clusters = new ArrayList<>();
        for (String value : record) {
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
            IntArrayList cluster = clusterMap.get(value);
            if (cluster == null || cluster.isEmpty()) {
                return Collections.emptyList();
            }
            clusters.add(cluster);
            attributeId++;
        }
        clusters.sort(Comparator.comparingInt(Collection::size));
        Collection<Integer> matching = null;
        for (IntArrayList cluster : clusters) {
            if (matching == null) {
                matching = cluster.clone();
            } else {
                matching.retainAll(cluster);
            }
        }
        return matching;
    }

    public Collection<Integer> getMatchingRecordsByClusterMapsAndCreateRemovalMap(Iterable<String> record, List<? extends Map<String, Collection<Integer>>> removalMap) {
        int attributeId = 0;
        List<IntArrayList> clusters = new ArrayList<>();
        for (String value : record) {
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
            IntArrayList cluster = clusterMap.get(value);
            if (cluster == null || cluster.isEmpty()) {
                return Collections.emptyList();
            }
            clusters.add(cluster);
            attributeId++;
        }
        clusters.sort(Comparator.comparingInt(Collection::size));
        Collection<Integer> matching = null;
        for (IntArrayList cluster : clusters) {
            if (matching == null) {
                matching = cluster.clone();
            } else {
                matching.retainAll(cluster);
            }
        }
        fillRemovalMap(record, matching, removalMap);
        return matching;

    public void removeRecords(Iterable<String> record, Collection<Integer> recordIds){
        int attributeId = 0;
        for(String value : record) {
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
            clusterMap.getOrDefault(value, new IntArrayList()).removeAll(recordIds);
            attributeId++;
        }
    }

    public void removeRecords(List<? extends Map<String, Collection<Integer>>> removalMap){
        int attributeId = 0;
        for(Map<String, Collection<Integer>> attribute : removalMap){
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
            for(Map.Entry<String, Collection<Integer>> entry : attribute.entrySet()){
                clusterMap.getOrDefault(entry.getKey(), new IntArrayList()).removeAll(entry.getValue());
        this.numRecords++;
        return recId;
    }
    }
 */

    public Collection<Integer> removeRecord(Iterable<String> record) {
        int attributeId = 0;
        List<IntArrayList> clusters = new ArrayList<>();
        for (String value : record) {
            HashMap<Integer, IntArrayList> clusterMap = clusterMaps.get(attributeId);
            Integer dictValue = dictionary.getOrAdd(value);
            IntArrayList cluster = clusterMap.get(dictValue);
            if (cluster == null || cluster.isEmpty()) {
                return Collections.emptyList();
            }
            clusters.add(cluster);
            attributeId++;
        }
        clusters.sort(Comparator.comparingInt(Collection::size));
        Set<Integer> matching = null;
        for (IntArrayList cluster : clusters) {
            if (matching == null) {
                matching = new HashSet<>(cluster);
            } else {
                matching.retainAll(cluster);
            }
        }
        Set<Integer> toRemove = matching;
        clusters.forEach(c -> c.removeAll(toRemove));
        return matching;
    }


    public void addRecords(Collection<? extends Iterable<String>> records) {
        if (records.size() > Integer.MAX_VALUE)
            throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + records.size() + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);

        for (Iterable<String> record : records) {
            addRecord(record);
        }
    }

    public Dictionary<String> getDictionary() {
        return dictionary;
    }
}

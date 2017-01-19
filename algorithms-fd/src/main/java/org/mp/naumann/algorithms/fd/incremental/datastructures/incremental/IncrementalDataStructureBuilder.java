package org.mp.naumann.algorithms.fd.incremental.datastructures.incremental;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.Dictionary;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IncrementalDataStructureBuilder implements DataStructureBuilder {

    private final IncrementalPLIBuilder pliBuilder;
    private final IncrementalFDConfiguration version;
    private final List<String> columns;
    private final Dictionary<String> dictionary;
    private final List<Integer> pliOrder;

    private List<? extends PositionListIndex> plis;
    private final MapCompressedRecords compressedRecords = new MapCompressedRecords();
    private int nextRecordId;

    public IncrementalDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns) {
        this(pliBuilder, version, columns, pliBuilder.getPliOrder());
    }

    public IncrementalDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns, List<Integer> pliOrder) {
        this.pliOrder = pliOrder;
        this.dictionary = new Dictionary<>(pliBuilder.isNullEqualNull());
        this.pliBuilder = new IncrementalPLIBuilder(pliOrder);
        this.version = version;
        this.columns = columns;
        this.nextRecordId = pliBuilder.getNumLastRecords();
        initialize(pliBuilder.getClusterMaps());
    }

    private void initialize(List<HashMap<String, IntArrayList>> oldClusterMaps) {
        List<Integer> inserted = IntStream.range(0, nextRecordId).boxed().collect(Collectors.toList());
        List<Map<Integer, IntArrayList>> clusterMaps = new ArrayList<>(oldClusterMaps.size());
        for (HashMap<String, IntArrayList> oldClusterMap : oldClusterMaps) {
            Map<Integer, IntArrayList> clusterMap = new HashMap<>();
            for (Entry<String, IntArrayList> cluster : oldClusterMap.entrySet()) {
                Integer dictValue = dictionary.getOrAdd(cluster.getKey());
                if (dictValue == null) continue;
                clusterMap.put(dictValue, cluster.getValue());
            }
            clusterMaps.add(clusterMap);
        }
        updateDataStructures(inserted, clusterMaps);
    }

    @Override
    public CompressedDiff update(Batch batch) {
        Set<Integer> inserted = new HashSet<>();
        IncrementalClusterMapBuilder clusterMapBuilder = new IncrementalClusterMapBuilder(columns.size(), nextRecordId, dictionary);
        List<InsertStatement> inserts = batch.getInsertStatements();
        for (InsertStatement insert : inserts) {
            Map<String, String> valueMap = insert.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            int id = clusterMapBuilder.addRecord(values);
            inserted.add(id);
        }
        nextRecordId = clusterMapBuilder.getNextRecordId();
        List<Map<Integer, IntArrayList>> clusterMaps = clusterMapBuilder.getClusterMaps();
        updateDataStructures(inserted, clusterMaps);
        if (version.usesClusterPruning() || version.usesEnhancedClusterPruning()) {
            Map<Integer, Set<Integer>> newClusters = null;
            if (version.usesEnhancedClusterPruning()) {
                newClusters = new HashMap<>(plis.size());
            }
            int i = 0;
            for (PositionListIndex pli : plis) {
                int attribute = pli.getAttribute();
                Set<Integer> clusterIds = clusterMaps.get(attribute).keySet();
                if (version.usesClusterPruning()) {
                    pli.setClustersWithNewRecords(clusterIds);
                }
                if (version.usesEnhancedClusterPruning()) {
                    newClusters.put(i, clusterIds);
                }
                i++;
            }
            if (version.usesEnhancedClusterPruning()) {
                Map<Integer, Set<Integer>> otherClustersWithNewRecords = newClusters;
                plis.forEach(pli -> pli.setOtherClustersWithNewRecords(otherClustersWithNewRecords));
            }
        }
        if(version.usesInnerClusterPruning()) {
            plis.forEach(pli -> pli.setNewRecords(inserted));
        }
        //TODO: deletes
        return CompressedDiff.buildDiff(inserted, null, version, compressedRecords);
    }

    private void updateDataStructures(Collection<Integer> inserted, List<Map<Integer, IntArrayList>> clusterMaps) {
        updatePlis(clusterMaps);
        updateCompressedRecords(clusterMaps, inserted);
    }

    private void updateCompressedRecords(List<Map<Integer, IntArrayList>> clusterMaps, Collection<Integer> inserted) {
        List<Map<Integer, Integer>> invertedPlis = invertPlis(clusterMaps);
        for (int recordId : inserted) {
            compressedRecords.put(recordId, fetchRecordFrom(recordId, invertedPlis));
        }
    }

    private static int[] fetchRecordFrom(int recordId, List<Map<Integer, Integer>> invertedPlis) {
        int numAttributes = invertedPlis.size();
        int[] record = new int[numAttributes];
        for (int i = 0; i < numAttributes; i++) {
            record[i] = invertedPlis.get(i).getOrDefault(recordId, PliUtils.UNIQUE_VALUE);
        }
        return record;
    }

    private List<Map<Integer, Integer>> invertPlis(List<Map<Integer, IntArrayList>> clusterMaps) {
        List<Map<Integer, Integer>> invertedPlis = new ArrayList<>();
        for (int clusterId : pliOrder) {
            Map<Integer, Integer> invertedPli = new HashMap<>();

            for (Entry<Integer, IntArrayList> cluster : clusterMaps.get(clusterId).entrySet()) {
                for (int recordId : cluster.getValue()) {
                    invertedPli.put(recordId, cluster.getKey());
                }
            }
            invertedPlis.add(invertedPli);
        }
        return invertedPlis;
    }

    private void updatePlis(List<Map<Integer, IntArrayList>> clusterMaps) {
        plis = pliBuilder.fetchPositionListIndexes(clusterMaps);
    }

    @Override
    public List<? extends PositionListIndex> getPlis() {
        return plis;
    }

    @Override
    public CompressedRecords getCompressedRecord() {
        return compressedRecords;
    }

    private static class MapCompressedRecords implements CompressedRecords {

        private final Map<Integer, int[]> compressedRecords = new HashMap<>();

        @Override
        public int[] get(int index) {
            return compressedRecords.get(index);
        }

        @Override
        public void fill(int index, int value) {
            Arrays.fill(compressedRecords.get(index), value);
        }

        @Override
        public int size() {
            return compressedRecords.size();
        }

        public void put(Integer id, int[] record) {
            compressedRecords.put(id, record);
        }
    }
}

package org.mp.naumann.algorithms.fd.incremental.datastructures.incremental;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.datastructures.AbstractStatementApplier;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.MapCompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.Dictionary;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;

import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IncrementalDataStructureBuilder implements DataStructureBuilder {

    private final IncrementalPLIBuilder pliBuilder;
    private final IncrementalFDConfiguration version;
    private final List<String> columns;
    private final List<Integer> pliOrder;
    private final IncrementalClusterMapBuilder clusterMapBuilder;
    private final Dictionary<String> dictionary;

    private List<? extends PositionListIndex> plis;
    private final MapCompressedRecords compressedRecords;

    public IncrementalDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns) {
        this(pliBuilder, version, columns, pliBuilder.getPliOrder());
    }

    public IncrementalDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns, List<Integer> pliOrder) {
        this.pliOrder = pliOrder;
        this.pliBuilder = new IncrementalPLIBuilder(pliOrder);
        this.version = version;
        this.columns = columns;
        this.dictionary = new Dictionary<>(pliBuilder.isNullEqualNull());
        int nextRecordId = pliBuilder.getNumLastRecords();
        this.compressedRecords = new MapCompressedRecords(nextRecordId, pliOrder.size());
        this.clusterMapBuilder = new IncrementalClusterMapBuilder(columns.size(), nextRecordId, dictionary);
        initialize(pliBuilder.getClusterMaps(), nextRecordId);
    }

    private void initialize(List<HashMap<String, IntArrayList>> oldClusterMaps, int nextRecordId) {
        List<Integer> inserted = IntStream.range(0, nextRecordId).boxed().collect(Collectors.toList());
        List<Map<Integer, IntArrayList>> clusterMaps = new ArrayList<>(oldClusterMaps.size());
        for (HashMap<String, IntArrayList> oldClusterMap : oldClusterMaps) {
            Map<Integer, IntArrayList> clusterMap = new HashMap<>();
            for (Entry<String, IntArrayList> cluster : oldClusterMap.entrySet()) {
                int dictValue = dictionary.getOrAdd(cluster.getKey());
                clusterMap.put(dictValue, cluster.getValue());
            }
            clusterMaps.add(clusterMap);
        }
        plis = pliBuilder.fetchPositionListIndexes(clusterMaps);
        List<Map<Integer, Integer>> invertedPlis = invertPlis(clusterMaps);
        for (int recordId : inserted) {
            compressedRecords.put(recordId, fetchRecordFrom(recordId, invertedPlis));
        }
    }

    @Override
    public CompressedDiff update(Batch batch) {
        clusterMapBuilder.flush();
        AbstractStatementApplier applier = new StatementApplier();
        for (Statement statement : batch.getStatements()) {
            statement.accept(applier);
        }
        Set<Integer> inserted = applier.getInserted();
        Set<Integer> deleted = applier.getDeleted();
        inserted.removeAll(deleted);

        Map<Integer, int[]> deletedDiff = new HashMap<>(deleted.size());
        deleted.forEach(i -> deletedDiff.put(i, getCompressedRecord(i)));

        updateDataStructures(inserted, deleted);

        Map<Integer, int[]> insertedDiff = new HashMap<>(inserted.size());
        inserted.forEach(i -> insertedDiff.put(i, getCompressedRecord(i)));

        return new CompressedDiff(insertedDiff, deletedDiff, new HashMap<>(0), new HashMap<>(0));
    }

    private void updateDataStructures(Collection<Integer> inserted, Collection<Integer> deleted) {
        updatePlis();
        updateCompressedRecords(inserted, deleted);
        if (version.usesClusterPruning() || version.usesEnhancedClusterPruning()) {
            List<Map<Integer, IntArrayList>> clusterMaps = clusterMapBuilder.getClusterMaps();
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
                    if (newClusters != null) newClusters.put(i, clusterIds);
                }
                i++;
            }
            if (version.usesEnhancedClusterPruning()) {
                Map<Integer, Set<Integer>> otherClustersWithNewRecords = newClusters;
                plis.forEach(pli -> pli.setOtherClustersWithNewRecords(otherClustersWithNewRecords));
            }
        }
        if (version.usesInnerClusterPruning()) {
            plis.forEach(pli -> pli.setNewRecords(inserted));
        }
    }

    private void updateCompressedRecords(Collection<Integer> inserted, Collection<Integer> deleted) {
        List<Map<Integer, IntArrayList>> clusterMaps = clusterMapBuilder.getClusterMaps();
        List<Map<Integer, Integer>> invertedPlis = invertPlis(clusterMaps);
        for (int recordId : inserted) {
            compressedRecords.put(recordId, fetchRecordFrom(recordId, invertedPlis));
        }
        deleted.forEach(compressedRecords::remove);
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

    private void updatePlis() {
        plis = pliBuilder.fetchPositionListIndexes(clusterMapBuilder.getClusterMaps());
    }

    @Override
    public List<? extends PositionListIndex> getPlis() {
        return plis;
    }

    @Override
    public CompressedRecords getCompressedRecords() {
        return compressedRecords;
    }

    @Override
    public int getNumRecords() {
        return compressedRecords.size();
    }

    private int[] getCompressedRecord(int record) {
        return version.usesPruningStrategy(PruningStrategy.ANNOTATION) || version.usesPruningStrategy(PruningStrategy.SIMPLE)? compressedRecords.get(record) : null;
    }

    private class StatementApplier extends AbstractStatementApplier {

        @Override
        protected int addRecord(Map<String, String> valueMap) {
            List<String> record = columns.stream().map(valueMap::get).collect(Collectors.toList());
            return clusterMapBuilder.addRecord(record);
        }

        private void logNotFoundWarning(Iterable<String> record) {
            FDLogger.log(Level.WARNING, String.format("Trying to remove %s, but there is no such record.", record.toString()));
        }

        @Override
        protected Collection<Integer> removeRecord(Map<String, String> valueMap) {
            List<String> record = columns.stream().map(valueMap::get).collect(Collectors.toList());

            // find records from previous batches in PLIs
            List<Collection<Integer>> clusters = new ArrayList<>();
            for (PositionListIndex pli : plis) {
                String value = record.get(pli.getAttribute());
                int dictValue = dictionary.getOrAdd(value);
                IntArrayList cluster = pli.getCluster(dictValue);
                if (cluster == null) {
                    cluster = new IntArrayList();
                }
                clusters.add(cluster);
            }
            Set<Integer> matching = CollectionUtils.intersection(clusters);

            // find records that were added in the current batch
            matching.addAll(clusterMapBuilder.removeRecord(record));

            clusters.forEach(c -> c.removeAll(matching));
            if (matching.isEmpty()) logNotFoundWarning(record);
            return matching;
        }
    }

}

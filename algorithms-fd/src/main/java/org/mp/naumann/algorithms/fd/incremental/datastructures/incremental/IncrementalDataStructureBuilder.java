package org.mp.naumann.algorithms.fd.incremental.datastructures.incremental;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.Factory;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.Cluster;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.IntArrayListCluster;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.datastructures.AbstractStatementApplier;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.MapCompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;

public class IncrementalDataStructureBuilder implements DataStructureBuilder {

    private final IncrementalPLIBuilder pliBuilder;
    private final IncrementalFDConfiguration version;
    private final List<String> columns;
    private final IntList pliOrder;
    private final IncrementalClusterMapBuilder clusterMapBuilder;
    private final Dictionary<String> dictionary;
    private final MapCompressedRecords compressedRecords;
    private List<? extends PositionListIndex> plis;

    public IncrementalDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns, Factory<Cluster> clusterFactory) {
        this.pliOrder = pliBuilder.getPliOrder();
        this.pliBuilder = new IncrementalPLIBuilder(pliOrder);
        this.version = version;
        this.columns = columns;
        this.dictionary = new Dictionary<>(pliBuilder.isNullEqualNull());
        int nextRecordId = pliBuilder.getNumLastRecords();
        this.compressedRecords = new MapCompressedRecords(nextRecordId, pliOrder.size());
        this.clusterMapBuilder = new IncrementalClusterMapBuilder(columns.size(), nextRecordId, dictionary,
            clusterFactory);
        initialize(pliBuilder.getClusterMaps(), nextRecordId, clusterFactory);
    }

    public IncrementalDataStructureBuilder(PLIBuilder pliBuilder,
        IncrementalFDConfiguration incrementalFDConfiguration, List<String> columns) {
        this(pliBuilder, incrementalFDConfiguration, columns, IntArrayListCluster::new);
    }

    private void initialize(List<Map<String, IntList>> oldClusterMaps, int nextRecordId,
        Factory<Cluster> clusterFactory) {
        IntList inserted = IntStream.range(0, nextRecordId).boxed().collect(Collectors.toCollection(IntArrayList::new));
        List<Int2ObjectMap<Cluster>> clusterMaps = new ArrayList<>(oldClusterMaps.size());
        for (Map<String, IntList> oldClusterMap : oldClusterMaps) {
            Int2ObjectMap<Cluster> clusterMap = new Int2ObjectOpenHashMap<>();
            for (Entry<String, IntList> cluster : oldClusterMap.entrySet()) {
                int dictValue = dictionary.getOrAdd(cluster.getKey());
                Cluster newCluster = clusterFactory.create();
                newCluster.addAll(cluster.getValue());
                clusterMap.put(dictValue, newCluster);
            }
            clusterMaps.add(clusterMap);
        }
        plis = pliBuilder.fetchPositionListIndexes(clusterMaps);
        List<Int2IntMap> invertedPlis = invertPlis(clusterMaps);
        for (int recordId : inserted) {
            compressedRecords.put(recordId, fetchRecordFrom(recordId, invertedPlis));
        }
    }

    @Override
    public CompressedDiff update(Batch batch) {
        int newRecordBoundary = clusterMapBuilder.getNextRecordId();
        clusterMapBuilder.flush();
        AbstractStatementApplier applier = new StatementApplier();
        for (Statement statement : batch.getStatements()) {
            statement.accept(applier);
        }
        IntSet inserted = applier.getInserted();
        IntSet deleted = applier.getDeleted();
        IntSet inserted_tmp = new IntOpenHashSet(inserted);
        inserted.removeAll(deleted);
        deleted.removeAll(inserted_tmp);

        Int2ObjectMap<int[]> deletedDiff = new Int2ObjectOpenHashMap<>(deleted.size());
        deleted.forEach((IntConsumer) i -> deletedDiff.put(i, getCompressedRecord(i)));

        updateDataStructures(inserted, deleted, newRecordBoundary);

        Int2ObjectMap<int[]> insertedDiff = new Int2ObjectOpenHashMap<>(inserted.size());
        inserted.forEach((IntConsumer) i -> insertedDiff.put(i, getCompressedRecord(i)));

        return new CompressedDiff(insertedDiff, deletedDiff, new Int2ObjectOpenHashMap<>(0), new Int2ObjectOpenHashMap<>(0));
    }

    private void updateDataStructures(IntCollection inserted, IntCollection deleted,
        int newRecordBoundary) {
        updatePlis();
        updateCompressedRecords(inserted, deleted);
        if (version.usesClusterPruning() || version.usesEnhancedClusterPruning()) {
            List<Int2ObjectMap<Cluster>> clusterMaps = clusterMapBuilder.getClusterMaps();
            Int2ObjectMap<IntSet> newClusters = null;
            if (version.usesEnhancedClusterPruning()) {
                newClusters = new Int2ObjectOpenHashMap<>(plis.size());
            }
            int i = 0;
            for (PositionListIndex pli : plis) {
                int attribute = pli.getAttribute();
                IntSet clusterIds = clusterMaps.get(attribute).keySet();
                if (version.usesClusterPruning()) {
                    pli.setClustersWithNewRecords(clusterIds);
                }
                if (version.usesEnhancedClusterPruning()) {
                    if (newClusters != null) newClusters.put(i, clusterIds);
                }
                i++;
            }
            if (version.usesEnhancedClusterPruning()) {
                Int2ObjectMap<IntSet> otherClustersWithNewRecords = newClusters;
                plis.forEach(pli -> pli.setOtherClustersWithNewRecords(otherClustersWithNewRecords));
            }
        }
        if (version.usesInnerClusterPruning()) {
            plis.forEach(pli -> pli.setNewRecordBoundary(newRecordBoundary));
        }
    }

    private void updateCompressedRecords(IntCollection inserted, IntCollection deleted) {
        List<Int2ObjectMap<Cluster>> clusterMaps = clusterMapBuilder.getClusterMaps();
        List<Int2IntMap> invertedPlis = invertPlis(clusterMaps);
        for (int recordId : inserted) {
            compressedRecords.put(recordId, fetchRecordFrom(recordId, invertedPlis));
        }
        deleted.forEach((IntConsumer) compressedRecords::remove);
    }

    private static int[] fetchRecordFrom(int recordId, List<Int2IntMap> invertedPlis) {
        int numAttributes = invertedPlis.size();
        int[] record = new int[numAttributes];
        for (int i = 0; i < numAttributes; i++) {
            record[i] = invertedPlis.get(i).getOrDefault(recordId, PliUtils.UNIQUE_VALUE);
        }
        return record;
    }

    private List<Int2IntMap> invertPlis(List<Int2ObjectMap<Cluster>> clusterMaps) {
        List<Int2IntMap> invertedPlis = new ArrayList<>();
        for (int clusterId : pliOrder) {
            Int2IntMap invertedPli = new Int2IntOpenHashMap();

            for (Int2ObjectMap.Entry<Cluster> cluster : clusterMaps.get(clusterId).int2ObjectEntrySet()) {
                for (int recordId : cluster.getValue()) {
                    invertedPli.put(recordId, cluster.getIntKey());
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
        return version.usesPruningStrategy(PruningStrategy.ANNOTATION) || version.usesPruningStrategy(PruningStrategy.SIMPLE) ? compressedRecords.get(record) : null;
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
        protected IntCollection removeRecord(Map<String, String> valueMap) {
            List<String> record = columns.stream().map(valueMap::get).collect(Collectors.toList());

            // find records from previous batches in PLIs
            List<Cluster> clusters = new ArrayList<>();
            for (PositionListIndex pli : plis) {
                String value = record.get(pli.getAttribute());
                int dictValue = dictionary.getOrAdd(value);
                Cluster cluster = pli.getCluster(dictValue);
                if (cluster == null) {
                    cluster = new IntArrayListCluster();
                }
                clusters.add(cluster);
            }
            IntSet matching = CollectionUtils.intersection(clusters);

            // find records that were added in the current batch
            matching.addAll(clusterMapBuilder.removeRecord(record));

            clusters.forEach(c -> c.removeAll(matching));
            if (matching.isEmpty()) logNotFoundWarning(record);
            return matching;
        }
    }

}

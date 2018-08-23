package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Map.Entry;
import java.util.function.IntConsumer;
import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.algorithms.fd.incremental.Factory;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.incremental.datastructures.AbstractStatementApplier;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RecomputeDataStructureBuilder implements DataStructureBuilder {

    private final RecomputePLIBuilder pliBuilder;
    private final IncrementalFDConfiguration version;
    private final List<String> columns;
    private final IntSet recordIds;

    private List<? extends PositionListIndex> plis;
    private CompressedRecords compressedRecords;

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns, Factory<Cluster> clusterFactory) {
        this.pliBuilder = new RecomputePLIBuilder(pliBuilder.getClusterMapBuilder(), pliBuilder.isNullEqualNull(), pliBuilder.getPliOrder(), clusterFactory);
        this.version = version;
        this.columns = columns;
        recordIds = IntStream.range(0, pliBuilder.getNumLastRecords()).boxed().collect(Collectors.toCollection(IntOpenHashSet::new));
        updateDataStructures();
    }

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder,
        IncrementalFDConfiguration incrementalFDConfiguration, List<String> columns) {
        this(pliBuilder, incrementalFDConfiguration, columns, IntArrayListCluster::new);
    }

    @Override
    public CompressedDiff update(Batch batch) {
        Benchmark benchmark = Benchmark.start("Recompute data structures", Benchmark.DEFAULT_LEVEL + 1);
        int newRecordBoundary = pliBuilder.getNextRecordId();
        AbstractStatementApplier applier = new StatementApplier();
        for (Statement statement : batch.getStatements()) {
            statement.accept(applier);
        }

        IntSet inserted = applier.getInserted();
        IntSet deleted = applier.getDeleted();
        IntSet inserted_tmp = new IntOpenHashSet(inserted);
        inserted.removeAll(deleted);
        deleted.removeAll(inserted_tmp);
        recordIds.addAll(inserted);
        recordIds.removeAll(deleted);

        benchmark.finishSubtask("Apply statements");
        Int2ObjectMap<int[]> deletedDiff = new Int2ObjectOpenHashMap<>(deleted.size());
        deleted.forEach((IntConsumer) i -> deletedDiff.put(i, getCompressedRecord(i)));

        benchmark.startSubtask();
        updateDataStructures(inserted, newRecordBoundary);
        benchmark.finishSubtask("Update data structures");

        Int2ObjectMap<int[]> insertedDiff = new Int2ObjectOpenHashMap<>(inserted.size());
        inserted.forEach((IntConsumer) i -> insertedDiff.put(i, getCompressedRecord(i)));

        benchmark.finish();
        return new CompressedDiff(insertedDiff, deletedDiff, new Int2ObjectOpenHashMap<>(0), new Int2ObjectOpenHashMap<>(0));
    }

    private int[] getCompressedRecord(int record) {
        return version.usesPruningStrategy(PruningStrategy.ANNOTATION) || version.usesPruningStrategy(PruningStrategy.SIMPLE) ? compressedRecords.get(record) : null;
    }

    private void updateDataStructures() {
        plis = pliBuilder.fetchPositionListIndexes();
        RecordCompressor recordCompressor = new ArrayRecordCompressor(recordIds, plis, pliBuilder.getNextRecordId());
        compressedRecords = recordCompressor.buildCompressedRecords();
    }

    private void updateDataStructures(IntSet inserted, int newRecordBoundary) {
        updateDataStructures();

        if (version.usesInnerClusterPruning()) {
            plis.forEach(pli -> pli.setNewRecordBoundary(newRecordBoundary));
        }

        if (version.usesClusterPruning() || version.usesEnhancedClusterPruning()) {
            Int2ObjectMap<IntSet> newClusters = null;
            if (version.usesEnhancedClusterPruning()) {
                newClusters = new Int2ObjectOpenHashMap<>(plis.size());
            }
            for (int i = 0; i < plis.size(); i++) {
                PositionListIndex pli = plis.get(i);
                IntSet clusterIds = extractClustersWithNewRecords(inserted, i,
                    newRecordBoundary);
                if (version.usesClusterPruning()) {
                    pli.setClustersWithNewRecords(clusterIds);
                }
                if (version.usesEnhancedClusterPruning()) {
                    if (newClusters != null) newClusters.put(i, clusterIds);
                }
            }
            if (version.usesEnhancedClusterPruning()) {
                Int2ObjectMap<IntSet> otherClustersWithNewRecords = newClusters;
                plis.forEach(pli -> pli.setOtherClustersWithNewRecords(otherClustersWithNewRecords));
            }
        }

    }

    private IntSet extractClustersWithNewRecords(IntCollection newRecords,
        int attribute, int newRecordBoundary) {
        PositionListIndex pli = plis.get(attribute);
        if (newRecords.size() > pli.size()) {
            IntSet clusterIds = new IntOpenHashSet();
            for (Int2ObjectMap.Entry<Cluster> cluster : pli.getClustersWithKey()) {
                int clusterId = cluster.getIntKey();
                if (cluster.getValue().largestElement() >= newRecordBoundary) {
                    clusterIds.add(clusterId);
                }
            }
            return clusterIds;
        } else {
            IntSet clusterIds = new IntOpenHashSet();
            for (int id : newRecords) {
                int clusterId = compressedRecords.get(id)[attribute];
                if (clusterId != PliUtils.UNIQUE_VALUE) {
                    clusterIds.add(clusterId);
                }
            }
            return clusterIds;
        }
    }

    public List<? extends PositionListIndex> getPlis() {
        return plis;
    }

    public CompressedRecords getCompressedRecords() {
        return compressedRecords;
    }

    @Override
    public int getNumRecords() {
        return recordIds.size();
    }

    private class StatementApplier extends AbstractStatementApplier {

        @Override
        protected int addRecord(Map<String, String> valueMap) {
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            return pliBuilder.addRecord(values);
        }

        @Override
        protected IntCollection removeRecord(Map<String, String> valueMap) {
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            return pliBuilder.removeRecord(values);
        }
    }
}

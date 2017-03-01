package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import org.mp.naumann.algorithms.benchmark.better.Benchmark;
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
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
    private final Set<Integer> recordIds;

    private List<? extends PositionListIndex> plis;
    private CompressedRecords compressedRecords;

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns) {
        this(pliBuilder, version, columns, pliBuilder.getPliOrder());
    }

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns, List<Integer> pliOrder) {
        this.pliBuilder = new RecomputePLIBuilder(pliBuilder.getClusterMapBuilder(), pliBuilder.isNullEqualNull(), pliOrder);
        this.version = version;
        this.columns = columns;
        recordIds = IntStream.range(0, pliBuilder.getNumLastRecords()).boxed().collect(Collectors.toSet());
        updateDataStructures();
    }

    @Override
    public CompressedDiff update(Batch batch) {
        Benchmark benchmark = Benchmark.start("Recompute data structures", Benchmark.DEFAULT_LEVEL + 1);
        AbstractStatementApplier applier = new StatementApplier();
        for (Statement statement : batch.getStatements()) {
            statement.accept(applier);
        }
        Set<Integer> inserted = applier.getInserted();
        Set<Integer> insertedUpdate = applier.getInsertedUpdate();
        inserted.addAll(insertedUpdate);
        recordIds.addAll(inserted);

        Set<Integer> deleted = applier.getDeleted();
        Set<Integer> deletedUpdate = applier.getDeletedUpdate();
        deleted.addAll(deletedUpdate);
        recordIds.removeAll(deleted);
        benchmark.finishSubtask("Apply statements");
        Map<Integer, int[]> deletedDiff = new HashMap<>(deleted.size());
        deleted.forEach(i -> deletedDiff.put(i, getCompressedRecord(i)));

        benchmark.startSubtask();
        updateDataStructures(inserted, deleted);
        benchmark.finishSubtask("Update data structures");

        Map<Integer, int[]> insertedDiff = new HashMap<>(inserted.size());
        inserted.forEach(i -> insertedDiff.put(i, getCompressedRecord(i)));

        benchmark.finish();
        return new CompressedDiff(insertedDiff, deletedDiff, new HashMap<>(0), new HashMap<>(0));
    }

    private int[] getCompressedRecord(int record) {
        return version.usesPruningStrategy(PruningStrategy.ANNOTATION) || version.usesPruningStrategy(PruningStrategy.SIMPLE)? compressedRecords.get(record) : null;
    }

    private void updateDataStructures() {
        plis = pliBuilder.fetchPositionListIndexes();
        RecordCompressor recordCompressor = new ArrayRecordCompressor(recordIds, plis, pliBuilder.getNumRecords());
        compressedRecords = recordCompressor.buildCompressedRecords();
    }

    private void updateDataStructures(Set<Integer> inserted, Set<Integer> deleted) {
        updateDataStructures();

        if (version.usesInnerClusterPruning()) {
            plis.forEach(pli -> pli.setNewRecords(inserted));
        }

        if (version.usesClusterPruning() || version.usesEnhancedClusterPruning()) {
            Map<Integer, Set<Integer>> newClusters = null;
            if (version.usesEnhancedClusterPruning()) {
                newClusters = new HashMap<>(plis.size());
            }
            for (int i = 0; i < plis.size(); i++) {
                PositionListIndex pli = plis.get(i);
                Set<Integer> clusterIds = extractClustersWithNewRecords(inserted, i);
                if (version.usesClusterPruning()) {
                    pli.setClustersWithNewRecords(clusterIds);
                }
                if (version.usesEnhancedClusterPruning()) {
                    newClusters.put(i, clusterIds);
                }
            }
            if (version.usesEnhancedClusterPruning()) {
                Map<Integer, Set<Integer>> otherClustersWithNewRecords = newClusters;
                plis.forEach(pli -> pli.setOtherClustersWithNewRecords(otherClustersWithNewRecords));
            }
        }

    }

    private Set<Integer> extractClustersWithNewRecords(Collection<Integer> newRecords, int attribute) {
        Set<Integer> clusterIds = new HashSet<>();
        for (int id : newRecords) {
            int clusterId = compressedRecords.get(id)[attribute];
            if (clusterId != PliUtils.UNIQUE_VALUE) {
                clusterIds.add(clusterId);
            }
        }
        return clusterIds;
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
        protected Collection<Integer> removeRecord(Map<String, String> valueMap) {
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            return pliBuilder.removeRecord(values);
        }
    }
}

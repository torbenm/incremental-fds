package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.datastructures.DataStructureBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.RecordCompressor;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RecomputeDataStructureBuilder implements DataStructureBuilder {

    private final RecomputePLIBuilder pliBuilder;
    private final IncrementalFDConfiguration version;
    private final List<String> columns;

    private List<? extends PositionListIndex> plis;
    private CompressedRecords compressedRecords;

    public RecomputeDataStructureBuilder(PLIBuilder pliBuilder, IncrementalFDConfiguration version, List<String> columns, List<Integer> pliOrder) {
        this.pliBuilder = new RecomputePLIBuilder(pliBuilder.getClusterMapBuilder(), pliBuilder.isNullEqualNull(), pliOrder);
        this.version = version;
        this.columns = columns;
    }

    public CompressedDiff update(Batch batch) {
        Set<Integer> inserted = new HashSet<>();
        List<InsertStatement> inserts = batch.getInsertStatements();
        for (InsertStatement insert : inserts) {
            Map<String, String> valueMap = insert.getValueMap();
            List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
            int id = pliBuilder.addRecord(values);
            inserted.add(id);
        }
        updateDataStructures(inserted);
        return CompressedDiff.buildDiff(inserted, version, compressedRecords);
    }

    private void updateDataStructures(Set<Integer> inserted) {
        plis = pliBuilder.fetchPositionListIndexes();
        int numRecords = pliBuilder.getNumRecords();

        compressedRecords = new ArrayCompressedRecords(RecordCompressor.fetchCompressedRecords(plis, numRecords));
        if (version.usesClusterPruning()) {
            for (int i = 0; i < plis.size(); i++) {
                PositionListIndex pli = plis.get(i);
                pli.setClustersWithNewRecords(inserted, compressedRecords, i);
            }
        }
    }

    public List<? extends PositionListIndex> getPlis() {
        return plis;
    }

    public CompressedRecords getCompressedRecord() {
        return compressedRecords;
    }

    private static class ArrayCompressedRecords implements CompressedRecords {
        private final int[][] compressedRecords;

        private ArrayCompressedRecords(int[][] compressedRecords) {
            this.compressedRecords = compressedRecords;
        }

        @Override
        public int[] get(int index) {
            return compressedRecords[index];
        }

        @Override
        public int size() {
            return compressedRecords.length;
        }
    }
}

package org.mp.naumann.algorithms.fd.incremental;

import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.RecordCompressor;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.processor.batch.Batch;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class IncrementalPLIBuilder {

	private final PLIBuilder pliBuilder;
	private final IncrementalFDVersion version;
	private final List<String> columns;

    private List<PositionListIndex> plis;
	private int[][] compressedRecords;

	public IncrementalPLIBuilder(PLIBuilder pliBuilder, IncrementalFDVersion version, List<String> columns) {
		this.pliBuilder = pliBuilder;
		this.version = version;
		this.columns = columns;
	}

	public CompressedDiff update(Batch batch) {
		List<InsertStatement> inserts = batch.getInsertStatements();
		for (InsertStatement insert : inserts) {
			Map<String, String> valueMap = insert.getValueMap();
			List<String> values = columns.stream().map(valueMap::get).collect(Collectors.toList());
			pliBuilder.addRecord(values);
		}
		updateDataStructures();
		return buildDiff(inserts.size());
	}

	private CompressedDiff buildDiff(int inserted) {
		int[][] insertedRecords = new int[inserted][];
		int i = 0;
        if(this.version.getPruningStrategy() == IncrementalFDVersion.PruningStrategy.SIMPLE){
            for(int id = compressedRecords.length - inserted; id < compressedRecords.length; id++) {
                insertedRecords[i] = compressedRecords[id];
                i++;
            }
        }
		int[][] deletedRecords = new int[0][];
		int[][] oldUpdatedRecords = new int[0][];
		int[][] newUpdatedRecords = new int[0][];
		return new CompressedDiff(insertedRecords, deletedRecords, oldUpdatedRecords, newUpdatedRecords);
	}

	private void updateDataStructures() {
		plis = pliBuilder.fetchPositionListIndexes();
		compressedRecords = RecordCompressor.fetchCompressedRecords(plis, pliBuilder.getNumLastRecords());
	}

    public List<PositionListIndex> getPlis() {
        return plis;
    }

    public int[][] getCompressedRecord() {
        return compressedRecords;
    }
}

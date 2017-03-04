package org.mp.naumann.algorithms.fd.incremental.datastructures;

import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.processor.batch.Batch;

import java.util.List;

public interface DataStructureBuilder {

    CompressedDiff update(Batch batch);

    List<? extends PositionListIndex> getPlis();

    CompressedRecords getCompressedRecords();

    int getNumRecords();
}

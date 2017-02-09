package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;

interface RecordCompressor {

    CompressedRecords buildCompressedRecords();
}

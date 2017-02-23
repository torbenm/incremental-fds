package org.mp.naumann.algorithms.fd.structures;

import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.utils.PliUtils;

import java.util.List;
import java.util.logging.Level;

public class RecordCompressor {

    public static int[][] fetchCompressedRecords(List<? extends IPositionListIndex> plis, int numRecords) {
        // Calculate inverted plis
        FDLogger.log(Level.FINER, "Inverting plis ...");
        int[][] invertedPlis = PliUtils.invert(plis, numRecords);
        // Extract the integer representations of all records from the inverted plis
        FDLogger.log(Level.FINER, "Extracting integer representations for the records ...");
        int[][] compressedRecords = new int[numRecords][];
        for (int recordId = 0; recordId < numRecords; recordId++)
            compressedRecords[recordId] = fetchRecordFrom(recordId, invertedPlis);
        invertedPlis = null;
        return compressedRecords;
    }

    private static int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
        int numAttributes = invertedPlis.length;
        int[] record = new int[numAttributes];
        for (int i = 0; i < numAttributes; i++)
            record[i] = invertedPlis[i][recordId];
        return record;
    }
}

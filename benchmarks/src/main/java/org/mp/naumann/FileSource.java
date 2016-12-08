package org.mp.naumann;

import org.apache.commons.io.FileUtils;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.SecureRandom;

import ResourceConnection.ResourceConnector;
import ResourceConnection.ResourceType;

public class FileSource {

    public static final String TEMP_DIR;

    private static final SecureRandom random = new SecureRandom();
    private static final String SESSION_ID;
    public static final String BASELINE_PATH, BASELINEANDONE_PATH, INSERTS_PATH;
    private static final String NL = "\n";

    static {
        SESSION_ID = new BigInteger(130, random).toString(16);
        TEMP_DIR = System.getProperty("java.io.tmpdir") + SESSION_ID + "/";
        BASELINE_PATH = TEMP_DIR + "benchmark.baseline.csv";
        BASELINEANDONE_PATH = TEMP_DIR + "benchmark.baselineandone.csv";
        INSERTS_PATH = TEMP_DIR + "benchmark.inserts.csv";
        new File(TEMP_DIR).mkdir();
    }

    private final String origin;
    private final int splittingLine;
    private final int batchSize;
    private boolean hasSplit = false;

    public FileSource(String origin, int splittingLine, int batchSize){

        this.origin = origin;
        this.splittingLine = splittingLine;
        this.batchSize = batchSize;
    }

    public void doSplit() throws IOException {
        if(!hasSplit){
            InputStream stream = ResourceConnector.getResource(ResourceType.BENCHMARK, origin);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream));
            PrintWriter baseline = new PrintWriter(BASELINE_PATH, "UTF-8");
            PrintWriter baselineandone = new PrintWriter(BASELINEANDONE_PATH, "UTF-8");
            PrintWriter inserts = new PrintWriter(INSERTS_PATH, "UTF-8");
            String line;
            long lineNumber = 0;
            while ((line = bufferedReader.readLine()) != null) {
                // Write header line for inserts
                if(lineNumber == 0) {
                    inserts.write(CsvFileBatchSource.ACTION_COLUMN_NAME+","+ line);
                    baseline.write(line);
                    baselineandone.write(line);
                }else {
                    if(lineNumber < splittingLine)
                        baseline.write(NL + line);
                    else
                        inserts.write(NL + "insert,"+line);

                    if(lineNumber < splittingLine + batchSize)
                        baselineandone.write(NL + line);
                }
                lineNumber++;
            }
            baseline.close();;
            baselineandone.close();
            inserts.close();
            bufferedReader.close();
            stream.close();
            hasSplit = true;
        }
    }

    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(TEMP_DIR));
    }
}

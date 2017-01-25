package org.mp.naumann;

import org.apache.commons.io.FileUtils;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.SecureRandom;

import ResourceConnection.ResourceConnector;

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

	public FileSource(String origin, int splittingLine, int batchSize) {

		this.origin = origin;
		this.splittingLine = splittingLine;
		this.batchSize = batchSize;
	}

	public void doSplit() throws IOException {
		if (!hasSplit) {
			try (BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(ResourceConnector.getResource(ResourceConnector.BENCHMARK, origin)));
					PrintWriter baseline = new PrintWriter(BASELINE_PATH, "UTF-8");
					PrintWriter baselineandone = new PrintWriter(BASELINEANDONE_PATH, "UTF-8");
					PrintWriter inserts = new PrintWriter(INSERTS_PATH, "UTF-8")) {
				String line;
				long lineNumber = 0;
				while ((line = bufferedReader.readLine()) != null) {
					// Write header line for inserts
					if (lineNumber == 0) {
						inserts.write(FixedSizeBatchSource.ACTION_COLUMN_NAME + "," + line);
						baseline.write(line);
						baselineandone.write(line);
					} else {
						if (lineNumber <= splittingLine) {
							baseline.write(NL + line);
						} else {
							inserts.write(NL + "insert," + line);
						}

						if (lineNumber < splittingLine + batchSize) {
							baselineandone.write(NL + line);
						}
					}
					lineNumber++;
				}
			}
			hasSplit = true;
			System.out.println("Finished preparing files.");
		}

	}

	public void cleanup() {
		try {
			FileUtils.deleteDirectory(new File(TEMP_DIR));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Finished deleting files.");
	}
}

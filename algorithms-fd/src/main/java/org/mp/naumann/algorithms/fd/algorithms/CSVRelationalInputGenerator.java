package org.mp.naumann.algorithms.fd.algorithms;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.csv.CSVFormat;

import com.google.common.io.Files;

public class CSVRelationalInputGenerator implements RelationalInputGenerator {

	private final File file;
	private static final CSVFormat FORMAT = CSVFormat.DEFAULT.withFirstRecordAsHeader();

	public CSVRelationalInputGenerator(File file) {
		this.file = file;
	}

	@Override
	public RelationalInput generateNewCopy() {
		try {
			return new CSVRelationalInput(Files.getNameWithoutExtension(file.getName()),
					FORMAT.parse(new FileReader(file)));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}

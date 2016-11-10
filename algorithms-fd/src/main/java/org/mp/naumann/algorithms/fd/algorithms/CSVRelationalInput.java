package org.mp.naumann.algorithms.fd.algorithms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVRelationalInput implements RelationalInput {

	private final CSVParser parser;
	private final Iterator<CSVRecord> iterator;
	private final String name;

	public CSVRelationalInput(String name, CSVParser parser) {
		this.parser = parser;
		this.name = name;
		this.iterator = parser.iterator();
		parser.getHeaderMap();
	}

	@Override
	public void close() throws Exception {
		parser.close();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public List<String> next() {
		int size = numberOfColumns();
		List<String> values = new ArrayList<>(size);
		CSVRecord record = iterator.next();
		for (int i = 0; i < size; i++) {
			values.add(record.get(i));
		}
		return values;
	}

	@Override
	public int numberOfColumns() {
		return parser.getHeaderMap().size();
	}

	@Override
	public String relationName() {
		return name;
	}

	@Override
	public List<String> columnNames() {
		return new ArrayList<>(parser.getHeaderMap().keySet());
	}

}

package org.mp.naumann.database.data;

public class StringColumn implements Column<String> {

	private final String name;

	public StringColumn(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Class<String> getType() {
		return String.class;
	}

}

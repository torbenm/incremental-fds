package org.mp.naumann.database.data;

public class GenericColumn<T> implements Column<T> {

    private String name;
    private Class<T> clazz;

    public GenericColumn(String name, Class<T> clazz) {
        this.name = name;
        this.clazz = clazz;
    }

    public String getName() {
        return name;
    }

	@Override
	public Class<T> getType() {
		return clazz;
	}
}
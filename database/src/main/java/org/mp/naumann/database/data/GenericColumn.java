package org.mp.naumann.database.data;

public class GenericColumn<T> implements Column<T> {

    private String name;
    private Class<T> clazz;

    public GenericColumn(String name, Class<T> clazz) {
        this.name = name;
        this.clazz = clazz;
    }

    public static GenericColumn<String> StringColumn(String name) {
        return new GenericColumn<>(name, String.class);
    }

    public String getName() {
        return name;
    }

    @Override
    public Class<T> getType() {
        return clazz;
    }
}

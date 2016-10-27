package org.mp.naumann.utils;

import java.util.Map;

public class GenericHelper {

    public static <T> T cast(Object obj, Class<T> clazz){
        if(clazz.isInstance(obj))
            return (T)obj;
        else
            throw new ClassCastException();
    }

    public static <T, S> Map<T, S> createGenericMap(Class<? extends Map> mapClass, Class<T> keyClass, Class<S> valueClass){
        try {
            return mapClass.newInstance();
        } catch (InstantiationException|IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}

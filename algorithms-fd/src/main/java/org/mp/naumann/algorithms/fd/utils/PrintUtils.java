package org.mp.naumann.algorithms.fd.utils;

import java.util.Collection;

public class PrintUtils {

    public static String toString(int[] array){
        StringBuilder b = new StringBuilder("[");
        for(int i : array){
            b.append(i);
            b.append(" ");
        }
        b.append("]");
        return b.toString();
    }

    public static void print(Object... objects){
        for(Object o : objects){
            System.out.print(o);
            System.out.print(" ");
        }
        System.out.println();
    }

    public static void print(Collection<String> strings){
        strings.forEach(System.out::println);
    }
}

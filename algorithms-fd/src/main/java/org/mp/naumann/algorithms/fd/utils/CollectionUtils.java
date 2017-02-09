package org.mp.naumann.algorithms.fd.utils;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Collection;

public class CollectionUtils {

	// Simply concatenate the elements of an IntArrayList
	public static String concat(IntArrayList integers, String separator) {
		if (integers == null)
			return "";
		
		StringBuilder buffer = new StringBuilder();
		
		for (int integer : integers) {
			buffer.append(integer);
			buffer.append(separator);
		}
		
		if (buffer.length() > separator.length())
			buffer.delete(buffer.length() - separator.length(), buffer.length());
		
		return buffer.toString();
	}

	public static<T> boolean intersects(Collection<T> one, Collection<T> two){
	    Collection<T> looped = one.size() < two.size() ? one : two;
	    Collection<T> searcher = one.size() < two.size() ? two : one;
	    for(T value : searcher){
	        if(looped.contains(value))
	            return true;
        }
        return false;
    }

}

package org.mp.naumann.utils;

import it.unimi.dsi.fastutil.ints.IntArrayList;

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

}

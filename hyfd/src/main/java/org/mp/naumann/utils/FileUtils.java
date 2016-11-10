package org.mp.naumann.utils;

public class FileUtils {

	public static void close(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}

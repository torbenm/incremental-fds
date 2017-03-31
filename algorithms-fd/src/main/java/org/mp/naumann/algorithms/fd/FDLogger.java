package org.mp.naumann.algorithms.fd;

import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class FDLogger {

    private static final Logger logger = Logger.getLogger("global");
    private static boolean silentMode = false;

    static {
        Handler systemOut = new ConsoleHandler();
        Formatter formatter = new CustomFormatter();
        systemOut.setFormatter(formatter);
        systemOut.setLevel(Level.ALL);
        logger.addHandler(systemOut);
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);
    }

    public static void setLevel(Level level) {
        logger.setLevel(level);
    }

    public static void log(Level level, Object object) {
        logger.log(level, object.toString());
    }

    public static void info(Object object) {
        log(Level.INFO, object);
    }

    public static void fine(Object object) {
        log(Level.FINE, object);
    }

    public static void finer(Object object) {
        log(Level.FINER, object);
    }

    public static void finest(Object object) {
        log(Level.FINEST, object);
    }

    public static void warn(Object object) {
        log(Level.WARNING, object);
    }

    public static void error(Object object) {
        log(Level.SEVERE, object);
    }

    private static class CustomFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            return record.getLevel().getName() + ": " + record.getMessage() + "\n";
        }

    }
}

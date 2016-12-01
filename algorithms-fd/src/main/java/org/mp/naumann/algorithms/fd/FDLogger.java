package org.mp.naumann.algorithms.fd;

import java.util.logging.*;

public class FDLogger {

    private static FunctionalDependencyAlgorithm currentAlgorithm;
    private static boolean silentMode = false;
    private static final Logger logger = Logger.getLogger("global");

    public static void setLevel(Level level) {
        logger.setLevel(level);
    }

    public static void setCurrentAlgorithm(FunctionalDependencyAlgorithm algorithm){
        currentAlgorithm = algorithm;
    }
    public static void log(Level level, String message){
        logger.log(level, message);
    }

    static {
        Handler systemOut = new ConsoleHandler();
        Formatter formatter = new CustomFormatter();
        systemOut.setFormatter(formatter);
        systemOut.setLevel( Level.ALL );
        logger.addHandler( systemOut );
        logger.setLevel( Level.ALL );
        logger.setUseParentHandlers(false);
    }

    private static class CustomFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            return record.getLevel().getName() + ": " + record.getMessage() + "\n";
        }

    }
}

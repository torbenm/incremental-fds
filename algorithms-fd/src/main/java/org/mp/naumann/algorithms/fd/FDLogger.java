package org.mp.naumann.algorithms.fd;

import java.util.logging.Level;

public class FDLogger {

    private static FunctionalDependencyAlgorithm currentAlgorithm;
    private static boolean silentMode = false;

    public static void silence(){
        silentMode = true;
    }
    public static void encourage(){
        silentMode = false;
    }
    public static void setCurrentAlgorithm(FunctionalDependencyAlgorithm algorithm){
        currentAlgorithm = algorithm;
    }
    public static void log(Level level, String message){
        if(!silentMode)
            System.out.print(message);
    }

    public static void logln(Level level, String line){
        if(!silentMode)
            System.out.println(line);
    }


}

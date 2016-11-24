package org.mp.naumann.algorithms.benchmark.speed;

import java.util.Arrays;
import java.util.stream.Collectors;

public class StringListSpeedInfo extends SpeedInfo {

    private final String[] infoString;

    public StringListSpeedInfo(String name, String... infoString){
        super(name);
        this.infoString = infoString;
    }

    public String[] getInfoString() {
        return infoString;
    }

    @Override
    protected String midOrEndEventToString(){
        return super.midOrEndEventToString() + " " +
                Arrays.stream(infoString).collect(Collectors.joining(", "));
    }

    @Override
    protected String startEventToString(){
        return super.startEventToString() + " " +
                Arrays.stream(infoString).collect(Collectors.joining(", "));
    }
}

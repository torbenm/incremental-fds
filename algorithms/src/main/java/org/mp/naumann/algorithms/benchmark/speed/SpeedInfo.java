package org.mp.naumann.algorithms.benchmark.speed;

import java.io.Serializable;

public class SpeedInfo implements Serializable {

    private final long time;
    private final String name;
    private long duration = -1L;

    public SpeedInfo(String name) {
        this.time = System.currentTimeMillis();
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public long getTime() {
        return time;
    }

    public long getDuration() {
        return duration;
    }

    void setDuration(long duration) {
        this.duration = duration;
    }

    @Override
    public final String toString(){
        return duration == -1L ? startEventToString() : midOrEndEventToString();
    }

    protected String startEventToString(){
        return String.format("Started %s at %d system time.", name, time);
    }

    protected String midOrEndEventToString(){
        return String.format("%s finished after %d ms. ", name, duration);
    }
}

package org.mp.naumann.algorithms.benchmark.speed;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class Speed {

    private static Speed current;
    public static Speed getCurrent(){
        return current;
    }

    public static void lap(SpeedInfo info){
        if(current != null)
            current.finishLap(info);
    }
    public static void lap(String info){
        lap(new SpeedInfo(info));
    }

    public static Speed create(){
        return new Speed();
    }

    private final LinkedList<SpeedInfo> speedEvents = new LinkedList<>();
    private boolean measuring = false;
    private SpeedEventListener eventListener;

    public Speed(){}
    public Speed(SpeedEventListener eventListener){
        this.eventListener = eventListener;
    }

    public void start(String info){
        start(new SpeedInfo(info));
    }
    public void start(SpeedInfo info){
        if(current != null)
            current.end("Aborted by new Speed Benchmark.");
        current = this;
        speedEvents.add(info);
        if(eventListener != null) eventListener.receiveEvent(speedEvents.peek());
        measuring = true;
    }
    public void end(String info){
        end(new SpeedInfo(info));
    }

    public void end(SpeedInfo info){
        info.setDuration(info.getTime() - speedEvents.peek().getTime());
        measuring = false;
        speedEvents.add(info);
        eventListener.receiveEvent(info);
    }

    public void finishLap(String info){
        finishLap(new SpeedInfo(info));
    }
    public void finishLap(SpeedInfo info){
        if(measuring){
            info.setDuration(info.getTime() - speedEvents.getLast().getTime());
            speedEvents.add(info);
            eventListener.receiveEvent(info);
        }
    }

    public List<String> toStringList(){
        return speedEvents.parallelStream().map(SpeedInfo::toString).collect(Collectors.toList());
    }

}

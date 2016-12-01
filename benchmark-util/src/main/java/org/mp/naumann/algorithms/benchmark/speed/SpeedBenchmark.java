package org.mp.naumann.algorithms.benchmark.speed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class SpeedBenchmark {

    private static final HashMap<BenchmarkLevel, LinkedList<SpeedEvent>> speedEvents = new HashMap<>();
    private static final HashMap<BenchmarkLevel, SpeedEvent> waitingEvents = new HashMap<>();
    private static final Set<SpeedEventListener> eventListenerSet = new HashSet<>();
    private static boolean enabled = false;

    public static void enable(){
        enabled = true;
    }

    public static void disable(){
        enabled = false;
    }

    public static void addEventListener(SpeedEventListener speedEventListener){
        eventListenerSet.add(speedEventListener);
    }
    public  static void removeEventListener(SpeedEventListener speedEventListener){
        eventListenerSet.remove(speedEventListener);
    }

    public static void begin(BenchmarkLevel level){
        if(enabled){
            SpeedEvent speedEvent = new SpeedEvent(level);
            waitingEvents.put(level, speedEvent);
        }
    }

    public static void end(BenchmarkLevel level, String message){
        if(enabled){
            SpeedEvent event = waitingEvents.remove(level);
            event.finish(message);
            storeEvent(event);
            notifyListeners(event);
        }
    }

    public static void lap(BenchmarkLevel level, String message){
        if(enabled){
            end(level, message);
            begin(level);
        }
    }
    private static void storeEvent(SpeedEvent event){
        if(!speedEvents.containsKey(event.getLevel()))
            speedEvents.put(event.getLevel(), new LinkedList<>());
        speedEvents.get(event.getLevel()).add(event);
    }

    private static void notifyListeners(SpeedEvent event){
        for(SpeedEventListener eventListener : eventListenerSet){
            eventListener.receiveEvent(event);
        }
    }



}

package org.mp.naumann.algorithms.benchmark.speed;

public class SpeedEvent {

    private final long startTime;
    private long endTime = -1L;
    private String message = "";
    private final BenchmarkLevel level;

    public SpeedEvent(long startTime, BenchmarkLevel level){
        this.startTime = startTime;
        this.level = level;
    }

    public SpeedEvent(BenchmarkLevel level){
        this(System.currentTimeMillis(), level);
    }

    public void finish(String message){
        this.endTime = System.currentTimeMillis();
        this.message = message;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getMessage() {
        return message;
    }

    public long getDuration(){
        return getDuration(endTime);
    }
    private long getDuration(long endTime){
        return endTime - startTime;
    }

    public BenchmarkLevel getLevel() {
        return level;
    }

    @Override
    public String toString(){
        if(endTime > 0){
            return message + " in "+getDuration()+" ms";
        }else{
            return "SpeedEvent still running since "+getDuration(System.currentTimeMillis())+ " ms";
        }
    }
}

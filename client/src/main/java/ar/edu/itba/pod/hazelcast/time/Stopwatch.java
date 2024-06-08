package ar.edu.itba.pod.hazelcast.time;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Stopwatch {

    private long startTime;
    private long endTime;

    private List<Map.Entry<String, Double>> timestamps;

    public Stopwatch() {
        this.timestamps = new ArrayList<>();
    }

    public void start() {
        this.startTime = System.nanoTime();
    }

    public double stop() {
        if (startTime == 0)
            throw new RuntimeException("Timer has not been started.");

        this.endTime = System.nanoTime();

        return getElapsedTimeInSeconds();
    }

    public double reset() {
        double elapsedTime = getElapsedTimeInSeconds();
        this.startTime = 0;
        this.endTime = 0;

        return elapsedTime;
    }

    public Map.Entry<String, Double> timestamp(String description) {
        double elapsedTime = getElapsedTimeInSeconds();
        Map.Entry<String, Double> lap = Map.entry(description, elapsedTime);

        timestamps.add(lap);

        return lap;
    }

    public List<Map.Entry<String, Double>> getTimestamp() {
        return timestamps;
    }

    private long getElapsedTimeInNanoseconds() {
        if (startTime == 0) {
            throw new RuntimeException("Timer has not been started.");
        }
        long currentTime = (endTime == 0) ? System.nanoTime() : endTime;
        return currentTime - startTime;
    }

    private double getElapsedTimeInSeconds() {
        return getElapsedTimeInNanoseconds() / 1_000_000_000.0;
    }
}

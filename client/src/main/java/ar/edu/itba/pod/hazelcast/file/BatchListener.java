package ar.edu.itba.pod.hazelcast.file;

import java.util.List;

@FunctionalInterface
public interface BatchListener {
    void onBatchParsedLine(List<String[]> lines);
}

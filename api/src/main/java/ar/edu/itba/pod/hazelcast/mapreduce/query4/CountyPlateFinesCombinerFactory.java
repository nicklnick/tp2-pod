package ar.edu.itba.pod.hazelcast.mapreduce.query4;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public class CountyPlateFinesCombinerFactory implements CombinerFactory<String, String, Map<String, Integer>> {
    @Override
    public Combiner<String, Map<String, Integer>> newCombiner(String s) {
        return new CountyPlateFinesCombiner();
    }

    private static class CountyPlateFinesCombiner extends Combiner<String, Map<String, Integer>> {

        private Map<String, Integer> countMap = new HashMap<>();

        @Override
        public void combine(String plate) {
            countMap.put(plate, countMap.getOrDefault(plate, 0) + 1);
        }

        @Override
        public Map<String, Integer> finalizeChunk() {
            return Map.copyOf(countMap);
        }

        @Override
        public void reset() {
            countMap.clear();
        }
    }
}

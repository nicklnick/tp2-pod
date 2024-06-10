package ar.edu.itba.pod.hazelcast.mapreduce.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public class CountyPlateFinesReducerFactory implements ReducerFactory<String, Map<String, Integer>, Map.Entry<String, Integer>> {
    @Override
    public Reducer<Map<String, Integer>, Map.Entry<String, Integer>> newReducer(String s) {
        return new CountyPlateFinesReducer();
    }

    private static class CountyPlateFinesReducer extends Reducer<Map<String, Integer>, Map.Entry<String, Integer>> {

        private Map<String, Integer> combinedMap = new HashMap<>();

        @Override
        public void reduce(Map<String, Integer> value) {
            for (Map.Entry<String, Integer> entry : value.entrySet()) {
                combinedMap.put(entry.getKey(), combinedMap.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
        }

        @Override
        public Map.Entry<String, Integer> finalizeReduce() {
            return combinedMap.entrySet().stream().max(Map.Entry.comparingByValue()).orElse(null);
        }
    }
}

package ar.edu.itba.pod.hazelcast.mapreduce.query4;

import ar.edu.itba.pod.hazelcast.models.PlateInfractions;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public class CountyPlateFinesReducerFactory implements ReducerFactory<String, Map<String, Integer>, PlateInfractions> {
    @Override
    public Reducer<Map<String, Integer>, PlateInfractions> newReducer(String s) {
        return new CountyPlateFinesReducer();
    }

    private static class CountyPlateFinesReducer extends Reducer<Map<String, Integer>, PlateInfractions> {

        private Map<String, Integer> combinedMap = new HashMap<>();

        @Override
        public void reduce(Map<String, Integer> value) {
            for (Map.Entry<String, Integer> entry : value.entrySet()) {
                combinedMap.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }

        @Override
        public PlateInfractions finalizeReduce() {
            return combinedMap
                    .entrySet()
                    .stream()
                    .map(e -> new PlateInfractions(e.getKey(), e.getValue()))
                    .max(Comparator.comparingInt(PlateInfractions::getInfractions))
                    .orElse(null);
        }
    }
}

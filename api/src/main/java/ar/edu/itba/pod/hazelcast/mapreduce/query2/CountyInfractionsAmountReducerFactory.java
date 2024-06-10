package ar.edu.itba.pod.hazelcast.mapreduce.query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation")
public class CountyInfractionsAmountReducerFactory implements ReducerFactory<String, String, Map<String, Integer>> {

    @Override
    public Reducer<String, Map<String, Integer>> newReducer(String string) {
        return new CountyInfractionsAmountReducer();
    }

    private static class CountyInfractionsAmountReducer extends Reducer<String, Map<String, Integer>> {
        private final Map<String, Integer> infractions = new HashMap<>();

        @Override
        public void reduce(String code) {
            System.out.println(code);
            Integer amount = infractions.getOrDefault(code, 0);
            infractions.put(code, amount + 1);
        }

        @Override
        public Map<String, Integer> finalizeReduce() {
            return infractions.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(3)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1,
                            LinkedHashMap::new
                    ));
        }
    }
}

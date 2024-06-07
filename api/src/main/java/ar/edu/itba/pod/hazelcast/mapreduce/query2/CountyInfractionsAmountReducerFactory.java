package ar.edu.itba.pod.hazelcast.mapreduce.query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation")
public class CountyInfractionsAmountReducerFactory {

    private final ReducerFactory<String, Integer, Map<Integer, Integer>> nycFactory = s -> new CountyInfractionsAmountReducer<>();

    private final ReducerFactory<String, String, Map<String, Integer>> chiFactory = s -> new CountyInfractionsAmountReducer<>();

    public ReducerFactory<String, String, Map<String, Integer>> getChiFactory(){
        return chiFactory;
    }

    public ReducerFactory<String, Integer, Map<Integer, Integer>> getNycFactory(){
        return nycFactory;
    }

    private static class CountyInfractionsAmountReducer<K> extends Reducer<K, Map<K, Integer>> {
        private final Map<K, Integer> infractions = new HashMap<>();

        @Override
        public void reduce(K code) {
            System.out.println(code);
            Integer amount = infractions.getOrDefault(code, 0);
            infractions.put(code, amount + 1);
        }

        @Override
        public Map<K, Integer> finalizeReduce() {
            return infractions.entrySet().stream()
                    .sorted(Map.Entry.<K, Integer>comparingByValue().reversed())
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

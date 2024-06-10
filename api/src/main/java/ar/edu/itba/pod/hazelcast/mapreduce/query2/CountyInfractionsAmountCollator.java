package ar.edu.itba.pod.hazelcast.mapreduce.query2;

import com.hazelcast.mapreduce.Collator;

import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("deprecation")
public class CountyInfractionsAmountCollator implements Collator<Map.Entry<String, Map<String, Integer>>, Map<String, Map<String, Integer>>> {

    @Override
    public Map<String, Map<String, Integer>> collate(Iterable<Map.Entry<String, Map<String, Integer>>> iterable) {
        Map<String,Map<String, Integer>> sortedMap = new TreeMap<>();
        for (Map.Entry<String, Map<String, Integer>> entry : iterable) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}

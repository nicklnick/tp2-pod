package ar.edu.itba.pod.hazelcast.mapreduce.query4;

import ar.edu.itba.pod.hazelcast.models.Pair;
import ar.edu.itba.pod.hazelcast.models.PlateInfractions;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@SuppressWarnings("deprecation")
public class CountyPlateCollator implements Collator<Map.Entry<Pair<String,String>, PlateInfractions>, Map<String, PlateInfractions>> {
    @Override
    public Map<String, PlateInfractions> collate(Iterable<Map.Entry<Pair<String,String>, PlateInfractions>> iterable) {
        return StreamSupport
                .stream(iterable.spliterator(), false)
                .map(Map.Entry::getValue)
                .collect(Collectors.toMap(
                        PlateInfractions::getCounty,
                        Function.identity(),
                        BinaryOperator.maxBy(Comparator.comparingInt(PlateInfractions::getInfractions)),
                        TreeMap::new));
    }
}

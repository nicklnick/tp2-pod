package ar.edu.itba.pod.hazelcast.mapreduce.query4;

import ar.edu.itba.pod.hazelcast.models.Pair;
import ar.edu.itba.pod.hazelcast.models.PlateInfractions;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public class CountyPlateFinesReducerFactory implements ReducerFactory<Pair<String, String>, PlateInfractions, PlateInfractions> {
    @Override
    public Reducer<PlateInfractions, PlateInfractions> newReducer(Pair<String,String> countyPlate) {
        return new CountyPlateFinesReducer(countyPlate);
    }

    private static class CountyPlateFinesReducer extends Reducer<PlateInfractions, PlateInfractions> {
        private String plate;
        private String county;
        private int count;

        public CountyPlateFinesReducer(Pair<String,String> countyPlate) {
            county = countyPlate.first();
            plate = countyPlate.second();
            count = 0;
        }

        @Override
        public void reduce(PlateInfractions value) {
            count += value.getInfractions();
        }

        @Override
        public PlateInfractions finalizeReduce() {
            return new PlateInfractions(plate, count, county);
        }
    }
}

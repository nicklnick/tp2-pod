package ar.edu.itba.pod.hazelcast.mapreduce.query4;

import ar.edu.itba.pod.hazelcast.models.Pair;
import ar.edu.itba.pod.hazelcast.models.PlateInfractions;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public class CountyPlateFinesCombinerFactory implements CombinerFactory<Pair<String,String>, PlateInfractions, PlateInfractions> {
    @Override
    public Combiner<PlateInfractions, PlateInfractions> newCombiner(Pair<String,String> countyPlate) {
        return new CountyPlateFinesCombiner(countyPlate);
    }

    private static class CountyPlateFinesCombiner extends Combiner<PlateInfractions, PlateInfractions> {

        private String plate;
        private String county;
        private int count;

        public CountyPlateFinesCombiner(Pair<String,String> countyPlate) {
            county = countyPlate.first();
            plate = countyPlate.second();
            count = 0;
        }

        @Override
        public void combine(PlateInfractions value) {
            count += value.getInfractions();
        }

        @Override
        public PlateInfractions finalizeChunk() {
            return new PlateInfractions(plate, count, county);
        }

        @Override
        public void reset() {
            count = 0;
        }
    }
}

package ar.edu.itba.pod.hazelcast.mapreduce.query5;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

@SuppressWarnings("deprecation")
public class InfractionFineAmountNycCombinerFactory implements CombinerFactory<String, Double, Double>{
    @Override
    public Combiner<Double, Double> newCombiner(String string) {
        return new InfractionFineAmountCombiner();
    }

    private static class InfractionFineAmountCombiner extends Combiner<Double, Double> {
        private double amount = 0;

        @Override
        public void combine(Double aDouble) {
            amount += aDouble;
        }

        @Override
        public Double finalizeChunk() {
            return amount;
        }

        @Override
        public void reset() {
            amount = 0;
        }
    }
}

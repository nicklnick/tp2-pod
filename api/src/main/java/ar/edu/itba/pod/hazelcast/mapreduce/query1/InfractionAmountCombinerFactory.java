package ar.edu.itba.pod.hazelcast.mapreduce.query1;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

@SuppressWarnings("deprecation")
public class InfractionAmountCombinerFactory implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String string) {
        return new InfractionAmountCombiner();
    }

    private static class InfractionAmountCombiner extends Combiner<Integer, Integer> {
        private int amount = 0;

        @Override
        public void combine(Integer integer) {
            amount += integer;
        }

        @Override
        public Integer finalizeChunk() {
            return amount;
        }

        @Override
        public void reset() {
            amount = 0;
        }
    }
}
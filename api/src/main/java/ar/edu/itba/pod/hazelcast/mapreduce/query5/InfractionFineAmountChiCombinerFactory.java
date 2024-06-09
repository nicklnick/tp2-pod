package ar.edu.itba.pod.hazelcast.mapreduce.query5;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class InfractionFineAmountChiCombinerFactory implements CombinerFactory<String, Integer, Integer> {
        @Override
        public Combiner<Integer, Integer> newCombiner(String string) {
            return new InfractionFineAmountChiCombiner();
        }

        private static class InfractionFineAmountChiCombiner extends Combiner<Integer, Integer> {
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

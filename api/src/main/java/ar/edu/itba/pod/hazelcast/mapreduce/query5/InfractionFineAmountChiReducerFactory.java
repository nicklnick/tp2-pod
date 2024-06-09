package ar.edu.itba.pod.hazelcast.mapreduce.query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

@SuppressWarnings("deprecation")
public class InfractionFineAmountChiReducerFactory implements ReducerFactory<String, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(String string) {
        return new InfractionFineAmountChiReducer();
    }

    private static class InfractionFineAmountChiReducer extends Reducer<Integer, Integer> {
        private volatile int amount = 0;

        @Override
        public void reduce(Integer integer) {
            amount += integer;
        }

        @Override
        public Integer finalizeReduce() {
            return amount;
        }
    }
}

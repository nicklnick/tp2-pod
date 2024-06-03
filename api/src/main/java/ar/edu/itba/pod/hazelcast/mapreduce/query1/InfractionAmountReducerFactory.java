package ar.edu.itba.pod.hazelcast.mapreduce.query1;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

@SuppressWarnings("deprecation")
public class InfractionAmountReducerFactory implements ReducerFactory<Integer, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(Integer integer) {
        return new InfractionAmountReducer();
    }

    private static class InfractionAmountReducer extends Reducer<Integer, Integer> {
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

package ar.edu.itba.pod.hazelcast.mapreduce.query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

@SuppressWarnings("deprecation")
public class InfractionFineAmountNycReducerFactory implements ReducerFactory<String, Double, Double>{
    @Override
    public Reducer<Double, Double> newReducer(String string) {
        return new InfractionFineAmountNycReducer();
    }

    private static class InfractionFineAmountNycReducer extends Reducer<Double, Double> {
        private double amount = 0;

        @Override
        public void reduce(Double aDouble) {
            amount += aDouble;
        }

        @Override
        public Double finalizeReduce() {
            return amount;
        }
    }
}

package ar.edu.itba.pod.hazelcast.mapreduce.query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

@SuppressWarnings("deprecation")
public class TicketAmountReducerFactory implements ReducerFactory<String, Double, Double> {
    @Override
    public Reducer<Double, Double> newReducer(String string) {
        return new TicketAmountReducer();
    }

    private static class TicketAmountReducer extends Reducer<Double, Double> {
        private double totalAgencyAmount = 0;

        @Override
        public void reduce(Double partialAmount) {
            totalAgencyAmount += partialAmount;
        }

        @Override
        public Double finalizeReduce() {
            return totalAgencyAmount;
        }
    }
}

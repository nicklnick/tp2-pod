package ar.edu.itba.pod.hazelcast.mapreduce.query3;

import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class TicketNycAmountMapper implements Mapper<String, TicketNyc, String, Double> {
    @Override
    public void map(String agency, TicketNyc ticketsNyc, Context<String, Double> context) {
        context.emit(agency, ticketsNyc.getFineAmount());
    }
}

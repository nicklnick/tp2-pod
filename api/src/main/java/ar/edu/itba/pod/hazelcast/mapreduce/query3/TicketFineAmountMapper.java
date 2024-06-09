package ar.edu.itba.pod.hazelcast.mapreduce.query3;

import ar.edu.itba.pod.hazelcast.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class TicketFineAmountMapper implements Mapper<String, Ticket, String, Double> {
    @Override
    public void map(String agency, Ticket ticket, Context<String, Double> context) {
        context.emit(agency, ticket.getFineAmount());

    }
}

package ar.edu.itba.pod.hazelcast.mapreduce.query5;

import ar.edu.itba.pod.hazelcast.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class InfractionFineAmountMapper implements Mapper<String, Ticket, String, Double> {
    @Override
    public void map(String infraction, Ticket ticket, Context<String, Double> context) {
        context.emit(infraction, ticket.getFineAmount());
    }
}

package ar.edu.itba.pod.hazelcast.mapreduce.query5;

import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class InfractionFineAmountNycMapper implements Mapper<String, TicketNyc, String, Double> {

    @Override
    public void map(String string, TicketNyc ticketNyc, Context<String, Double> context) {
        context.emit(string, ticketNyc.getFineAmount());
    }
}

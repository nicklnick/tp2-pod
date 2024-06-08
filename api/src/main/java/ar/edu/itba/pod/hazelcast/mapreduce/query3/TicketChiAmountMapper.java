package ar.edu.itba.pod.hazelcast.mapreduce.query3;

import ar.edu.itba.pod.hazelcast.models.TicketChi;
import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class TicketChiAmountMapper implements Mapper<String, TicketChi, String, Double> {
    @Override
    public void map(String agency, TicketChi ticketChi, Context<String, Double> context) {
        context.emit(agency, ticketChi.getFineAmount().doubleValue());
    }
}

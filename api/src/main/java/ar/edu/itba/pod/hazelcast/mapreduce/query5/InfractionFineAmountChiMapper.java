package ar.edu.itba.pod.hazelcast.mapreduce.query5;

import ar.edu.itba.pod.hazelcast.models.TicketChi;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class InfractionFineAmountChiMapper implements Mapper<String, TicketChi, String, Integer> {
    @Override
    public void map(String string, TicketChi ticketChi, Context<String, Integer> context) {
        context.emit(string, ticketChi.getFineAmount());
    }
}

package ar.edu.itba.pod.hazelcast.mapreduce.query2;

import ar.edu.itba.pod.hazelcast.models.TicketChi;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class ChiCountyInfractionsMapper implements Mapper<String, TicketChi, String, String> {
    @Override
    public void map(String county, TicketChi ticketChi, Context<String,String> context) {
        context.emit(county, ticketChi.getCode());
    }
}

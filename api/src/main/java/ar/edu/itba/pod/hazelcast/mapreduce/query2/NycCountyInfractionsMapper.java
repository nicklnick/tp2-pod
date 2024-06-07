package ar.edu.itba.pod.hazelcast.mapreduce.query2;

import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class NycCountyInfractionsMapper implements Mapper<String, TicketNyc, String, Integer> {
    @Override
    public void map(String county, TicketNyc ticketNyc, Context<String,Integer> context) {
        context.emit(county, ticketNyc.getCode());
    }
}

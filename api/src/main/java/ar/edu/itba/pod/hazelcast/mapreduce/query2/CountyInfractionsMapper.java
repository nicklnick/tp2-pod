package ar.edu.itba.pod.hazelcast.mapreduce.query2;

import ar.edu.itba.pod.hazelcast.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class CountyInfractionsMapper implements Mapper<String, Ticket, String, String> {
    @Override
    public void map(String county, Ticket ticket, Context<String,String> context) {
        context.emit(county, ticket.getCode());
    }
}

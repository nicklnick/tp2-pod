package ar.edu.itba.pod.hazelcast.mapreduce.query4;

import ar.edu.itba.pod.hazelcast.models.Pair;
import ar.edu.itba.pod.hazelcast.models.PlateInfractions;
import ar.edu.itba.pod.hazelcast.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;

@SuppressWarnings("deprecation")
public class CountyPlatesMapper implements Mapper<String, Ticket, Pair<String,String>, PlateInfractions> {
    @Override
    public void map(String county, Ticket ticket, Context<Pair<String, String>, PlateInfractions> context) {
        context.emit(new Pair<>(ticket.getCountyName(), ticket.getPlate()), new PlateInfractions(ticket.getPlate(), 1, ticket.getCountyName()));
    }

}

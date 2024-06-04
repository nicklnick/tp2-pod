package ar.edu.itba.pod.hazelcast.mapreduce.query1;

import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class InfractionCodeMapper implements Mapper<String, TicketNyc, String, Integer> {

    @Override
    public void map(String string, TicketNyc ticketNyc, Context<String, Integer> context) {
        context.emit(string, 1);
    }
}

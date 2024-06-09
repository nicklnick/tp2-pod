package ar.edu.itba.pod.hazelcast.mapreduce.query1;

import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class InfractionNycCodeMapper implements Mapper<String, TicketNyc, String, Integer> {

    @Override
    public void map(String s, TicketNyc ticketNyc, Context<String, Integer> context) {
        context.emit(s, 1);
    }
}

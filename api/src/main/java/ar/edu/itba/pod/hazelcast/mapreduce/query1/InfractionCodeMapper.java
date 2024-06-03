package ar.edu.itba.pod.hazelcast.mapreduce.query1;

import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class InfractionCodeMapper implements Mapper<Integer, TicketNyc, Integer, Integer>{
    @Override
    public void map(Integer integer, TicketNyc ticketNyc, Context<Integer, Integer> context) {
        context.emit(ticketNyc.getCode(), 1);
    }
}

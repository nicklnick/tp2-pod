package ar.edu.itba.pod.hazelcast.mapreduce.query1;

import ar.edu.itba.pod.hazelcast.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class InfractionCodeMapper implements Mapper<String, Ticket, String, Integer> {
    @Override
    public void map(String s, Ticket ticket, Context<String, Integer> context) {
        context.emit(s, 1);
    }
}

package ar.edu.itba.pod.hazelcast.mapreduce.query1;

import ar.edu.itba.pod.hazelcast.models.TicketChi;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class InfractionChiCodeMapper implements Mapper<String, TicketChi, String, Integer> {

        @Override
        public void map(String string, TicketChi ticketChi, Context<String, Integer> context) {
            context.emit(string, 1);
        }
}

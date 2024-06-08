package ar.edu.itba.pod.hazelcast.mapreduce.query3;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

@SuppressWarnings("deprecation")
public class TicketAmountCollator {

    public Collator<Map.Entry<String, Double>, Map<String, Double>> getCollator() {
        return values -> {
            double totalAmountsSum = 0;
            for (Map.Entry<String, Double> entry : values) {
                totalAmountsSum += entry.getValue();
            }

            Map<String, Double> agencyRatios = new HashMap<>();
            for (Map.Entry<String, Double> entry : values) {
                String agency = entry.getKey();
                double ratio = entry.getValue() / totalAmountsSum;
                agencyRatios.put(agency, ratio);
            }

            return agencyRatios;
        };
    }

}

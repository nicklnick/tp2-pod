package ar.edu.itba.pod.hazelcast.cities;

import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionAmountCombinerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionAmountReducerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionChiCodeMapper;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionNycCodeMapper;
import ar.edu.itba.pod.hazelcast.mapreduce.query2.ChiCountyInfractionsMapper;
import ar.edu.itba.pod.hazelcast.mapreduce.query2.CountyInfractionsAmountCollator;
import ar.edu.itba.pod.hazelcast.mapreduce.query2.CountyInfractionsAmountReducerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query2.NycCountyInfractionsMapper;
import ar.edu.itba.pod.hazelcast.models.TicketChi;
import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("deprecation")
public enum CityQuerySolver {
    NYC {
        @Override
        public Map<String, Integer> solveQueryOne(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, TicketNyc> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, TicketNyc> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, TicketNyc> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Integer>> future = job
                    .mapper(new InfractionNycCodeMapper())
                    .combiner(new InfractionAmountCombinerFactory())
                    .reducer(new InfractionAmountReducerFactory())
                    .submit();

            // print result
            try {
                return future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public Map<String,List<String>> solveQueryTwo(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, TicketNyc> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final IMap<Integer,String> infractions = hazelcastInstance.getMap(CityData.NYC.getMapName());

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, TicketNyc> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, TicketNyc> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Map<Integer,Integer>>> future = job
                    .mapper(new NycCountyInfractionsMapper())
                    .reducer(new CountyInfractionsAmountReducerFactory().getNycFactory())
                    .submit(new CountyInfractionsAmountCollator().getNycCollator());

            // print result
            try {
                Map<String,List<String>> results = new LinkedHashMap<>();
                for (Map.Entry<String,Map<Integer,Integer>> entry : future.get().entrySet()) {
                    List<String> infractionsDescription = new ArrayList<>();
                    for (Integer key : entry.getValue().keySet()) {
                        infractionsDescription.add(infractions.get(key));
                    }
                    results.put(entry.getKey(), infractionsDescription);
                }
                return results;

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    },
    CHI {
        @Override
        public Map<String, Integer> solveQueryOne(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, TicketChi> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, TicketChi> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, TicketChi> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Integer>> future = job
                    .mapper(new InfractionChiCodeMapper())
                    .combiner(new InfractionAmountCombinerFactory())
                    .reducer(new InfractionAmountReducerFactory())
                    .submit();

            // print result
            try {
                return future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public Map<String,List<String>> solveQueryTwo(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, TicketChi> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final IMap<Integer,String> infractions = hazelcastInstance.getMap(CityData.CHI.getMapName());

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, TicketChi> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, TicketChi> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Map<String,Integer>>> future = job
                    .mapper(new ChiCountyInfractionsMapper())
                    .reducer(new CountyInfractionsAmountReducerFactory().getChiFactory())
                    .submit(new CountyInfractionsAmountCollator().getChiCollator());
            // print result
            try {
                Map<String,List<String>> results = new LinkedHashMap<>();
                for (Map.Entry<String,Map<String,Integer>> entry : future.get().entrySet()) {
                    List<String> infractionsDescription = new ArrayList<>();
                    for (String key : entry.getValue().keySet()) {
                        infractionsDescription.add(infractions.get(key));
                    }
                    results.put(entry.getKey(), infractionsDescription);
                }
                return results;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    };

    public abstract Map<String, Integer> solveQueryOne(HazelcastInstance hazelcastInstance);
    public abstract Map<String, List<String>>  solveQueryTwo(HazelcastInstance hazelcastInstance);
}

package ar.edu.itba.pod.hazelcast.cities;

import ar.edu.itba.pod.hazelcast.mapreduce.query1.*;
import ar.edu.itba.pod.hazelcast.mapreduce.query2.CountyInfractionsAmountCollator;
import ar.edu.itba.pod.hazelcast.mapreduce.query2.CountyInfractionsAmountReducerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query2.CountyInfractionsMapper;
import ar.edu.itba.pod.hazelcast.mapreduce.query3.*;
import ar.edu.itba.pod.hazelcast.mapreduce.query4.CountyPlateCollator;
import ar.edu.itba.pod.hazelcast.mapreduce.query4.CountyPlatesMapper;
import ar.edu.itba.pod.hazelcast.mapreduce.query4.CountyPlateFinesCombinerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query4.CountyPlateFinesReducerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query5.*;
import ar.edu.itba.pod.hazelcast.models.PlateInfractions;
import ar.edu.itba.pod.hazelcast.models.Ticket;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;

@SuppressWarnings("deprecation")
public enum CityQuerySolver {
    NYC {
        @Override
        public Map<String, Integer> solveQueryOne(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, Ticket> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Integer>> future = job
                    .mapper(new InfractionCodeMapper())
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
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final IMap<Integer,String> infractions = hazelcastInstance.getMap(CityData.NYC.getMapName());

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, Ticket> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Map<String, Integer>>> future = job
                    .mapper(new CountyInfractionsMapper())
                    .reducer(new CountyInfractionsAmountReducerFactory())
                    .submit(new CountyInfractionsAmountCollator());

            try {
                Map<String, List<String>> results = new LinkedHashMap<>();
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
        @Override
        public Map<String, Double> solveQueryThree(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, Ticket> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Double>> future = job
                    .mapper(new TicketFineAmountMapper())
                    .reducer(new TicketAmountReducerFactory())
                    .submit(new TicketAmountCollator().getCollator());

            // print result
            try {
                return future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        }

        public Map<String, PlateInfractions> solveQueryFour(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);
            final KeyValueSource<String, Ticket> source = KeyValueSource.fromMultiMap(mm);
            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, PlateInfractions>> future = job
                    .mapper(new CountyPlatesMapper())
                    .combiner(new CountyPlateFinesCombinerFactory())
                    .reducer(new CountyPlateFinesReducerFactory())
                    .submit(new CountyPlateCollator());

            try {
                return future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        public Map<String, Integer> solveQueryFive(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, Ticket> source= KeyValueSource.fromMultiMap(mm);
            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Double>> future = job
                    .mapper(new InfractionFineAmountMapper())
                    .combiner(new InfractionFineAmountCombinerFactory())
                    .reducer(new InfractionFineAmountReducerFactory())
                    .submit();

            try{
                final Map<String, Double> totalFineAmountPerInfractionType = future.get();

                final Map<String, Integer> totalTicketsPerInfractionType = solveQueryOne(hazelcastInstance);

                final Map<String, Integer> meanFineAmountPerInfractionType = new HashMap<>();

                totalTicketsPerInfractionType.entrySet().parallelStream().forEach(
                        entry -> {
                            String infractionType = entry.getKey();
                            Integer totalTickets = entry.getValue();
                            Double totalFineAmount = totalFineAmountPerInfractionType.get(infractionType);
                            meanFineAmountPerInfractionType.put(infractionType, totalFineAmount.intValue() / totalTickets);
                        }
                );

                return meanFineAmountPerInfractionType;

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    },
    CHI {
        @Override
        public Map<String, Integer> solveQueryOne(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, Ticket> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Integer>> future = job
                    .mapper(new InfractionCodeMapper())
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
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final IMap<Integer,String> infractions = hazelcastInstance.getMap(CityData.CHI.getMapName());

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, Ticket> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Map<String,Integer>>> future = job
                    .mapper(new CountyInfractionsMapper())
                    .reducer(new CountyInfractionsAmountReducerFactory())
                    .submit(new CountyInfractionsAmountCollator());

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

        @Override
        public Map<String, Double> solveQueryThree(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);

            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, Ticket> source = KeyValueSource.fromMultiMap(mm);

            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Double>> future = job
                    .mapper(new TicketFineAmountMapper())
                    .reducer(new TicketAmountReducerFactory())
                    .submit(new TicketAmountCollator().getCollator());

            // print result
            try {
                return future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        }

        @Override
        public Map<String, PlateInfractions> solveQueryFour(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);
            final KeyValueSource<String, Ticket> source = KeyValueSource.fromMultiMap(mm);
            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, PlateInfractions>> future = job
                    .mapper(new CountyPlatesMapper())
                    .combiner(new CountyPlateFinesCombinerFactory())
                    .reducer(new CountyPlateFinesReducerFactory())
                    .submit(new CountyPlateCollator());

            try {
                return future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public Map<String, Integer> solveQueryFive(HazelcastInstance hazelcastInstance) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final JobTracker jobTracker = hazelcastInstance.getJobTracker(CredentialUtils.GROUP_NAME);

            final KeyValueSource<String, Ticket> source= KeyValueSource.fromMultiMap(mm);
            final Job<String, Ticket> job = jobTracker.newJob(source);

            final ICompletableFuture<Map<String, Double>> future = job
                    .mapper(new InfractionFineAmountMapper())
                    .combiner(new InfractionFineAmountCombinerFactory())
                    .reducer(new InfractionFineAmountReducerFactory())
                    .submit();

            try{
                final Map<String, Double> totalFineAmountPerInfractionType = future.get();

                final Map<String, Integer> totalTicketsPerInfractionType = solveQueryOne(hazelcastInstance);

                Map<String, Integer> meanFineAmountPerInfractionType = new HashMap<>();

                totalTicketsPerInfractionType.entrySet().parallelStream().forEach(
                        entry -> {
                            String infractionType = entry.getKey();
                            Integer totalTickets = entry.getValue();
                            Double totalFineAmount = totalFineAmountPerInfractionType.get(infractionType);
                            meanFineAmountPerInfractionType.put(infractionType, totalFineAmount.intValue() / totalTickets);
                        }
                );

                return meanFineAmountPerInfractionType;

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    };

    public abstract Map<String, Integer> solveQueryOne(HazelcastInstance hazelcastInstance);
    public abstract Map<String, List<String>> solveQueryTwo(HazelcastInstance hazelcastInstance);
    public abstract Map<String, Double> solveQueryThree(HazelcastInstance hazelcastInstance);
    public abstract Map<String, PlateInfractions> solveQueryFour(HazelcastInstance hazelcastInstance);
    public abstract Map<String, Integer> solveQueryFive(HazelcastInstance hazelcastInstance);

}

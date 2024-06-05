package ar.edu.itba.pod.hazelcast.cities;

import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionAmountCombinerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionAmountReducerFactory;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionChiCodeMapper;
import ar.edu.itba.pod.hazelcast.mapreduce.query1.InfractionNycCodeMapper;
import ar.edu.itba.pod.hazelcast.models.TicketChi;
import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

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
    };

    public abstract Map<String, Integer> solveQueryOne(HazelcastInstance hazelcastInstance);
}

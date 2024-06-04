package ar.edu.itba.pod.hazelcast.util;

public class HazelcastNamespaceUtils {
    public static final String MAP_INFRACTIONS_NYC = "g1-infractionsNyc";
    public static final String MAP_INFRACTIONS_CHI = "g1-infractionsChi";

    private HazelcastNamespaceUtils() {
        throw new AssertionError("This class should not be instantiated");
    }
}

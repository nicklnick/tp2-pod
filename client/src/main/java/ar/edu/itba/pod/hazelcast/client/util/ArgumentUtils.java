package ar.edu.itba.pod.hazelcast.client.util;

public class ArgumentUtils {
    // Default arguments
    public static final String ADDRESSES = "addresses";
    public static final String IN_PATH = "inPath";
    public static final String OUT_PATH = "outPath";

    // Query specific arguments
    public static final String CITY = "city";


    private ArgumentUtils() {
        throw new AssertionError("This class should not be instantiated");
    }
}

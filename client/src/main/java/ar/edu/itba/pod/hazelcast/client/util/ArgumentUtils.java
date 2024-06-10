package ar.edu.itba.pod.hazelcast.client.util;

public class ArgumentUtils {
    // Default arguments
    public static final String ADDRESSES = "addresses";
    public static final String IN_PATH = "inPath";
    public static final String OUT_PATH = "outPath";
    public static final String CITY = "city";


    // Query specific arguments
    public static final String N_AGENCIES = "n";
    public static final String FROM_DATE = "from";
    public static final String TO_DATE = "to";



    private ArgumentUtils() {
        throw new AssertionError("This class should not be instantiated");
    }
}

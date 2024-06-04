package ar.edu.itba.pod.hazelcast.file.util;

public class FileUtils {
    public static final String TICKETS_NYC_CSV = "ticketsNYC.csv";
    public static final String TICKETS_CHI_CSV = "ticketsCHI.csv";

    public static final String INFRACTIONS_NYC_CSV = "infractionsNYC.csv";
    public static final String INFRACTIONS_CHI_CSV = "infractionsCHI.csv";

    public static final String DELIMITER = "/";

    private FileUtils() {
        throw new AssertionError("This class should not be instantiated");
    }
}

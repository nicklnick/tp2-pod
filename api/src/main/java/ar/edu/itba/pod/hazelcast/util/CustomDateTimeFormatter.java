package ar.edu.itba.pod.hazelcast.util;

import java.time.format.DateTimeFormatter;

public class CustomDateTimeFormatter {
    public static final DateTimeFormatter D_M_YYYY_WITH_SLASH = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS_WITH_SLASH = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private CustomDateTimeFormatter() {
        throw new AssertionError("This class should not be instantiated");
    }
}

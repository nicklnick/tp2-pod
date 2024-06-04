package ar.edu.itba.pod.hazelcast.cities;

import ar.edu.itba.pod.hazelcast.file.util.FileUtils;
import ar.edu.itba.pod.hazelcast.util.HazelcastNamespaceUtils;

public enum CityData {
    NYC("nyc", FileUtils.INFRACTIONS_NYC_CSV, FileUtils.TICKETS_NYC_CSV, HazelcastNamespaceUtils.MAP_INFRACTIONS_NYC),
    CHI("chi", FileUtils.INFRACTIONS_CHI_CSV, FileUtils.TICKETS_CHI_CSV, HazelcastNamespaceUtils.MAP_INFRACTIONS_CHI);

    private final String city;
    private final String infractionsFile;
    private final String ticketsFile;
    private final String mapName;

    CityData(String city, String infractionsFile, String ticketsFile, String mapName) {
        this.city = city;
        this.infractionsFile = infractionsFile;
        this.ticketsFile = ticketsFile;
        this.mapName = mapName;
    }

    public static CityData getCityData(String city) {
        for (CityData cityAction : CityData.values())
            if (cityAction.city.equalsIgnoreCase(city))
                return cityAction;

        throw new IllegalArgumentException("City not found");
    }

    public String getInfractionsFile() {
        return infractionsFile;
    }

    public String getTicketsFile() {
        return ticketsFile;
    }

    public String getMapName() {
        return mapName;
    }
}

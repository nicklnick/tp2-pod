package ar.edu.itba.pod.hazelcast.cities;

import ar.edu.itba.pod.hazelcast.file.CsvFileReader;
import ar.edu.itba.pod.hazelcast.file.util.FileUtils;
import ar.edu.itba.pod.hazelcast.models.*;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.function.BiFunction;
import java.util.function.Function;

public enum CityCsvLoader {
    NYC {

        @Override
        public void loadQueryOne(HazelcastInstance hazelcastInstance, String dirPath) {
            NYC.loadTicketsAndInfractions(
                    hazelcastInstance,
                    dirPath,
                    CityData.NYC,
                    (hzInstance) -> NYC.loadInfractions(hzInstance, dirPath, CityData.NYC),
                    Ticket::fromTicketNycCsv,
                    (infractionsMap, ticket) -> infractionsMap.get(ticket.getCode())
            );
        }

        @Override
        public void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath) {
            NYC.loadTicketsAndInfractions(
                    hazelcastInstance,
                    dirPath,
                    CityData.NYC,
                    (hzInstance) -> NYC.loadInfractions(hzInstance, dirPath, CityData.NYC),
                    Ticket::fromTicketNycCsv,
                    (infractionsMap, ticket) -> ticket.getCountyName()
            );
        }

        @Override
        public void loadQueryThree(HazelcastInstance hazelcastInstance, String dirPath) {
            NYC.loadTickets(hazelcastInstance, dirPath, CityData.NYC, Ticket::fromTicketNycCsv, Ticket::getIssuingAgency);
        }

        @Override
        public void loadQueryFour(HazelcastInstance hazelcastInstance, String dirPath, LocalDate from, LocalDate to) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getTicketsFile());

            CsvFileReader.readRows(tickets, line -> {
                final Ticket ticket = Ticket.fromTicketNycCsv(line);
                if(!ticket.getIssueDate().isBefore(from.atStartOfDay()) && ticket.getIssueDate().isBefore(to.plusDays(1).atStartOfDay()))
                    mm.put(ticket.getCountyName(), ticket);
            });
        }

        @Override
        public void loadQueryFive(HazelcastInstance hazelcastInstance, String dirPath) {
            loadQueryOne(hazelcastInstance, dirPath);
        }
    },
    CHI {
        @Override
        public void loadQueryOne(HazelcastInstance hazelcastInstance, String dirPath) {
            CHI.loadTicketsAndInfractions(
                    hazelcastInstance,
                    dirPath,
                    CityData.CHI,
                    (hzInstance) -> CHI.loadInfractions(hzInstance, dirPath, CityData.CHI),
                    Ticket::fromTicketChiCsv,
                    (infractionsMap, ticket) -> infractionsMap.get(ticket.getCode())
            );
        }

        @Override
        public void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath) {
            CHI.loadTicketsAndInfractions(
                    hazelcastInstance,
                    dirPath,
                    CityData.CHI,
                    (hzInstance) -> CHI.loadInfractions(hzInstance, dirPath, CityData.CHI),
                    Ticket::fromTicketChiCsv,
                    (infractionsMap, ticket) -> ticket.getCountyName()
            );
        }

        @Override
        public void loadQueryThree(HazelcastInstance hazelcastInstance, String dirPath) {
            CHI.loadTickets(hazelcastInstance, dirPath, CityData.CHI, Ticket::fromTicketChiCsv, Ticket::getIssuingAgency);
        }

        @Override
        public void loadQueryFour(HazelcastInstance hazelcastInstance, String dirPath, LocalDate from, LocalDate to) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getTicketsFile());

            CsvFileReader.readRows(tickets, line -> {
                final Ticket ticket = Ticket.fromTicketChiCsv(line);
                if(!ticket.getIssueDate().isBefore(from.atStartOfDay()) && ticket.getIssueDate().isBefore(to.plusDays(1).atStartOfDay()))
                    mm.put(ticket.getCountyName(), ticket);
            });
        }

        @Override
        public void loadQueryFive(HazelcastInstance hazelcastInstance, String dirPath) {
            loadQueryOne(hazelcastInstance, dirPath);
        }
    }
    ;

    private IMap<String, String> loadInfractions(
            HazelcastInstance hazelcastInstance,
            String dirPath,
            CityData cityData
    ) {
        final IMap<String, String> infractions = hazelcastInstance.getMap(cityData.getMapName());
        final String infractionsFile = String.join(FileUtils.DELIMITER, dirPath, cityData.getInfractionsFile());

        CsvFileReader.readRows(infractionsFile, line -> {
            final Infraction infraction = Infraction.fromInfractionCsv(line);
            infractions.put(infraction.getCode(), infraction.getDescription());
        });

        return infractions;
    }

    /**
     * Load tickets into a MultiMap.
     *
     * @param hazelcastInstance
     * @param dirPath Directory path where the CSV files are located
     * @param cityData
     * @param ticketMapper Mapping function from CSV line to Ticket
     * @param keyMapper Mapping function from Ticket to key
     * @param <K> Key type
     */
    private <K> void loadTickets(
            HazelcastInstance hazelcastInstance,
            String dirPath,
            CityData cityData,
            Function<String[], Ticket> ticketMapper,
            Function<Ticket, K> keyMapper
    ) {
        final MultiMap<K, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
        final String tickets = String.join(FileUtils.DELIMITER, dirPath, cityData.getTicketsFile());

        CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
            final Ticket ticket = ticketMapper.apply(line);
            mm.put(keyMapper.apply(ticket), ticket);
        }));
    }

    /**
     * Load tickets and infractions into a MultiMap.
     * @param hazelcastInstance
     * @param dirPath Directory path where the CSV files are located
     * @param cityData
     * @param infractionsLoader Function to load infractions map
     * @param ticketMapper Mapping function from CSV line to Ticket
     * @param keyMapper Mapping function from Ticket and infractions map to key
     * @param <K> Key type
     */
    public <K> void loadTicketsAndInfractions(
            HazelcastInstance hazelcastInstance,
            String dirPath,
            CityData cityData,
            Function<HazelcastInstance, IMap<String, String>> infractionsLoader,
            Function<String[], Ticket> ticketMapper,
            BiFunction<IMap<String, String>, Ticket, K> keyMapper
    ) {
        final IMap<String, String> infractions = infractionsLoader.apply(hazelcastInstance);

        final MultiMap<K, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
        final String tickets = String.join(FileUtils.DELIMITER, dirPath, cityData.getTicketsFile());

        CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
            final Ticket ticket = ticketMapper.apply(line);
            mm.put(keyMapper.apply(infractions, ticket), ticket);
        }));
    }


    public abstract void loadQueryOne(HazelcastInstance hazelcastInstance, String dirPath);
    public abstract void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath);
    public abstract void loadQueryThree(HazelcastInstance hazelcastInstance, String dirPath);
    public abstract void loadQueryFour(HazelcastInstance hazelcastInstance, String dirPath, LocalDate from, LocalDate to);
    public abstract void loadQueryFive(HazelcastInstance hazelcastInstance, String dirPath);
}

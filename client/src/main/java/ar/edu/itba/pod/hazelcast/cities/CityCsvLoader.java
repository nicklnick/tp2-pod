package ar.edu.itba.pod.hazelcast.cities;

import ar.edu.itba.pod.hazelcast.file.CsvFileReader;
import ar.edu.itba.pod.hazelcast.file.util.FileUtils;
import ar.edu.itba.pod.hazelcast.models.*;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public enum CityCsvLoader {
    NYC {

        @Override
        public void loadQueryOne(HazelcastInstance hazelcastInstance, String dirPath) {
            // get infractionsXXX.csv and ticketsXXX.csv
            final IMap<String, String> infractions = NYC.loadInfractions(hazelcastInstance, dirPath, CityData.NYC);

            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getTicketsFile());

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final Ticket ticket = Ticket.fromTicketNycCsv(line);
                mm.put(infractions.get(ticket.getCode()), ticket);
            }));
        }

        @Override
        public void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath) {
            NYC.loadInfractions(hazelcastInstance, dirPath, CityData.NYC);

            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getTicketsFile());

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final Ticket ticket = Ticket.fromTicketNycCsv(line);
                mm.put(ticket.getCountyName(), ticket);
            }));
        }

        @Override
        public void loadQueryThree(HazelcastInstance hazelcastInstance, String dirPath) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getTicketsFile());

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final Ticket ticket = Ticket.fromTicketNycCsv(line);
                mm.put(ticket.getIssuingAgency(), ticket);
            }));
        }

        @Override
        public void loadQueryFour(HazelcastInstance hazelcastInstance, String dirPath) {
            //loadQueryTwo(hazelcastInstance, dirPath);


            // TODO: Filter by date

            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getTicketsFile());

            //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            CsvFileReader.readRows(tickets, line -> {
                final Ticket ticket = Ticket.fromTicketNycCsv(line);

                /*
                try {
                    Date issueDate = sdf.parse(ticket.getIssueDate());
                    if (!issueDate.before() && !issueDate.after()) {
                    */
                        mm.put(ticket.getCountyName(), ticket);
                    /*}
                } catch (ParseException e) {
                    throw new IllegalArgumentException(e);
                }*/
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
            final IMap<String, String> infractions = CHI.loadInfractions(hazelcastInstance, dirPath, CityData.CHI);

            // get infractionsXXX.csv and ticketsXXX.csv
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getTicketsFile());

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final Ticket ticket = Ticket.fromTicketChiCsv(line);
                mm.put(infractions.get(ticket.getCode()), ticket);
            }));
        }

        @Override
        public void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath) {
            CHI.loadInfractions(hazelcastInstance, dirPath, CityData.CHI);

            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getTicketsFile());

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final Ticket ticket = Ticket.fromTicketChiCsv(line);
                mm.put(ticket.getCountyName(), ticket);
            }));
        }

        @Override
        public void loadQueryThree(HazelcastInstance hazelcastInstance, String dirPath) {
            final MultiMap<String, Ticket> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getTicketsFile());

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final Ticket ticket = Ticket.fromTicketChiCsv(line);
                mm.put(ticket.getIssuingAgency(), ticket);
            }));
        }

        @Override
        public void loadQueryFour(HazelcastInstance hazelcastInstance, String dirPath) {
            loadQueryTwo(hazelcastInstance, dirPath);

            /*
            // TODO: Filter by date
            CHI.loadInfractions(hazelcastInstance, dirPath, CityData.CHI);

            final MultiMap<String, TicketChi> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getTicketsFile());

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final TicketChi ticket = TicketChi.fromTicketChiCsv(line);
                try {
                    Date issueDate = sdf.parse(ticket.getIssueDate());
                    if (!issueDate.before() && !issueDate.after()) {
                        mm.put(ticket.getCountyName(), ticket);
                    }
                } catch (ParseException e) {
                    throw new IllegalArgumentException(e);
                }
            }));
            */
        }

        @Override
        public void loadQueryFive(HazelcastInstance hazelcastInstance, String dirPath) {
            loadQueryOne(hazelcastInstance, dirPath);
        }
    }
    ;

    private IMap<String, String> loadInfractions(HazelcastInstance hazelcastInstance, String dirPath, CityData cityData) {
        final IMap<String, String> infractions = hazelcastInstance.getMap(cityData.getMapName());
        final String infractionsFile = String.join(FileUtils.DELIMITER, dirPath, cityData.getInfractionsFile());

        CsvFileReader.readRows(infractionsFile, line -> {
            final Infraction infraction = Infraction.fromInfractionCsv(line);
            infractions.put(infraction.getCode(), infraction.getDescription());
        });

        return infractions;
    }

    public abstract void loadQueryOne(HazelcastInstance hazelcastInstance, String dirPath);
    public abstract void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath);
    public abstract void loadQueryThree(HazelcastInstance hazelcastInstance, String dirPath);
    public abstract void loadQueryFour(HazelcastInstance hazelcastInstance, String dirPath);
    public abstract void loadQueryFive(HazelcastInstance hazelcastInstance, String dirPath);
}

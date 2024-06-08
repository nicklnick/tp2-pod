package ar.edu.itba.pod.hazelcast.cities;

import ar.edu.itba.pod.hazelcast.file.CsvFileReader;
import ar.edu.itba.pod.hazelcast.file.util.FileUtils;
import ar.edu.itba.pod.hazelcast.models.InfractionChi;
import ar.edu.itba.pod.hazelcast.models.InfractionNyc;
import ar.edu.itba.pod.hazelcast.models.TicketChi;
import ar.edu.itba.pod.hazelcast.models.TicketNyc;
import ar.edu.itba.pod.hazelcast.util.CredentialUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

public enum CityCsvLoader {
    NYC {
        @Override
        public void loadQueryOne(HazelcastInstance hazelcastInstance, String dirPath) {
            // get infractionsXXX.csv and ticketsXXX.csv
            final IMap<Integer, String> infractions = hazelcastInstance.getMap(CityData.NYC.getMapName());
            final String infractionsFile = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getInfractionsFile());

            final MultiMap<String, TicketNyc> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getTicketsFile());

            CsvFileReader.readRows(infractionsFile, line -> {
                final InfractionNyc infraction = InfractionNyc.fromInfractionNycCsv(line);
                infractions.put(infraction.getCode(), infraction.getDescription());
            });

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final TicketNyc ticket = TicketNyc.fromTicketNycCsv(line);
                mm.put(infractions.get(ticket.getCode()), ticket);
            }));
        }

        @Override
        public void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath) {
            final IMap<Integer, String> infractions = hazelcastInstance.getMap(CityData.NYC.getMapName());
            final String infractionsFile = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getInfractionsFile());

            final MultiMap<String, TicketNyc> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.NYC.getTicketsFile());

            // read infractions and tickets
            CsvFileReader.readRows(infractionsFile, line -> {
                final InfractionNyc infraction = InfractionNyc.fromInfractionNycCsv(line);
                infractions.put(infraction.getCode(), infraction.getDescription());
            });

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final TicketNyc ticket = TicketNyc.fromTicketNycCsv(line);
                mm.put(ticket.getCountyName(), ticket);
            }));
        }
    },
    CHI {
        @Override
        public void loadQueryOne(HazelcastInstance hazelcastInstance, String dirPath) {
            // get infractionsXXX.csv and ticketsXXX.csv
            final IMap<String, String> infractions = hazelcastInstance.getMap(CityData.CHI.getMapName());
            final String infractionsFile = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getInfractionsFile());

            final MultiMap<String, TicketChi> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getTicketsFile());

            // read infractions and tickets
            CsvFileReader.readRows(infractionsFile, line -> {
                final InfractionChi infraction = InfractionChi.fromInfractionChiCsv(line);
                infractions.put(infraction.getCode(), infraction.getDescription());
            });

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final TicketChi ticket = TicketChi.fromTicketChiCsv(line);
                mm.put(infractions.get(ticket.getCode()), ticket);
            }));
        }

        @Override
        public void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath) {
            final IMap<String, String> infractions = hazelcastInstance.getMap(CityData.CHI.getMapName());
            final String infractionsFile = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getInfractionsFile());

            final MultiMap<String, TicketChi> mm = hazelcastInstance.getMultiMap(CredentialUtils.GROUP_NAME);
            final String tickets = String.join(FileUtils.DELIMITER, dirPath, CityData.CHI.getTicketsFile());

            // read infractions and tickets
            CsvFileReader.readRows(infractionsFile, line -> {
                final InfractionChi infraction = InfractionChi.fromInfractionChiCsv(line);
                infractions.put(infraction.getCode(), infraction.getDescription());
            });

            CsvFileReader.batchReadRows(tickets, lines -> lines.parallelStream().forEach(line -> {
                final TicketChi ticket = TicketChi.fromTicketChiCsv(line);
                mm.put(ticket.getCountyName(), ticket);
            }));
        }
    }
    ;

    public abstract void loadQueryOne(HazelcastInstance hazelcastInstance, String dirPath);
    public abstract void loadQueryTwo(HazelcastInstance hazelcastInstance, String dirPath);
}

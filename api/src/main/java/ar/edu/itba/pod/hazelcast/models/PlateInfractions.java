package ar.edu.itba.pod.hazelcast.models;

import java.io.Serializable;

public record PlateInfractions(String getPlate, int getInfractions, String getCounty) implements Serializable {
}

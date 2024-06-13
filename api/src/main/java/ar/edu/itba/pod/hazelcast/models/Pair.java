package ar.edu.itba.pod.hazelcast.models;

import java.io.Serializable;

public record Pair<T,U>(T first, U second) implements Serializable {
}

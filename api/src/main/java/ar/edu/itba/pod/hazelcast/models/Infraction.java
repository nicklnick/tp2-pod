package ar.edu.itba.pod.hazelcast.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Infraction implements DataSerializable {
    private String code;
    private String description;

    public Infraction() {
        // for Hazelcast
    }

    public Infraction(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static Infraction fromInfractionCsv(String[] line) {
        return new Infraction(
                line[0],
                line[1]
        );
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(code);
        out.writeUTF(description);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        code = in.readUTF();
        description = in.readUTF();
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}

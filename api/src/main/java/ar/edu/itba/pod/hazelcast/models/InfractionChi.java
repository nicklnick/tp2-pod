package ar.edu.itba.pod.hazelcast.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class InfractionChi implements DataSerializable {
    private String code;
    private String description;

    public InfractionChi() {
        // for Hazelcast
    }

    public InfractionChi(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static InfractionChi fromInfractionChiCsv(String[] line) {
        return new InfractionChi(
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

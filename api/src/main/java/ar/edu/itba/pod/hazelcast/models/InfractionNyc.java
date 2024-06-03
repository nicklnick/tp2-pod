package ar.edu.itba.pod.hazelcast.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class InfractionNyc implements DataSerializable {
    private Integer code;
    private String description;

    public InfractionNyc() {
        // for Hazelcast
    }

    public InfractionNyc(Integer code, String description) {
        this.code = code;
        this.description = description;
    }

    public static InfractionNyc fromInfractionNycCsv(String[] line) {
        return new InfractionNyc(
                Integer.parseInt(line[0]),
                line[1]);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(code);
        out.writeUTF(description);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        code = in.readInt();
        description = in.readUTF();
    }

    public Integer getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}

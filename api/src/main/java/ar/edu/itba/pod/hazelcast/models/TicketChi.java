package ar.edu.itba.pod.hazelcast.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class TicketChi implements DataSerializable {
    private String issueDate;
    private String plate;
    private String code;
    private String issuingAgency;
    private Integer fineAmount;
    private String countyName;

    public TicketChi() {
        // for Hazelcast
    }

    public TicketChi(String issueDate,
                     String plate,
                     String code,
                     String issuingAgency,
                     Integer fineAmount,
                     String countyName
    ) {
        this.issueDate = issueDate;
        this.plate = plate;
        this.code = code;
        this.issuingAgency = issuingAgency;
        this.fineAmount = fineAmount;
        this.countyName = countyName;
    }

    public static TicketChi fromTicketChiCsv(String[] line) {
        return new TicketChi(
                line[0],
                line[1],
                line[2],
                line[3],
                Integer.parseInt(line[4]),
                line[5]
        );
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(issueDate);
        out.writeUTF(plate);
        out.writeUTF(code);
        out.writeUTF(issuingAgency);
        out.writeInt(fineAmount);
        out.writeUTF(countyName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        issueDate = in.readUTF();
        plate = in.readUTF();
        code = in.readUTF();
        issuingAgency = in.readUTF();
        fineAmount = in.readInt();
        countyName = in.readUTF();
    }

    @Override
    public String toString() {
        return "TicketChi{" +
                "plate='" + plate + '\'' +
                ", issueDate=" + issueDate +
                ", code=" + code +
                ", fineAmount=" + fineAmount +
                ", countyName='" + countyName + '\'' +
                ", issuingAgency='" + issuingAgency + '\'' +
                '}';
    }

    public String getPlate() {
        return plate;
    }

    public String getIssueDate() {
        return issueDate;
    }

    public String getCode() {
        return code;
    }

    public Integer getFineAmount() {
        return fineAmount;
    }

    public String getCountyName() {
        return countyName;
    }

    public String getIssuingAgency() {
        return issuingAgency;
    }
}

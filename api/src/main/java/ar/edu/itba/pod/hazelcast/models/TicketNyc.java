package ar.edu.itba.pod.hazelcast.models;

import ar.edu.itba.pod.hazelcast.util.CustomDateTimeFormatter;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDate;

public class TicketNyc implements DataSerializable {
    private String plate;
    private LocalDate issueDate;
    private Integer code;
    private Double fineAmount;
    private String countyName;
    private String issuingAgency;

    public TicketNyc() {
        // for Hazelcast
    }

    public TicketNyc(String plate,
                     LocalDate issueDate,
                     Integer code,
                     Double fineAmount,
                     String countyName,
                     String issuingAgency
    ) {
        this.plate = plate;
        this.issueDate = issueDate;
        this.code = code;
        this.fineAmount = fineAmount;
        this.countyName = countyName;
        this.issuingAgency = issuingAgency;
    }

    public static TicketNyc fromTicketNycCsv(String[] line) {
        return new TicketNyc(
                line[0],
                LocalDate.parse(line[1], CustomDateTimeFormatter.D_M_YYYY_WITH_SLASH),
                Integer.parseInt(line[2]),
                Double.parseDouble(line[3]),
                line[4],
                line[5]);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(plate);
        out.writeLong(issueDate.toEpochDay());
        out.writeInt(code);
        out.writeDouble(fineAmount);
        out.writeUTF(countyName);
        out.writeUTF(issuingAgency);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        plate = in.readUTF();
        issueDate = LocalDate.ofEpochDay(in.readLong());
        code = in.readInt();
        fineAmount = in.readDouble();
        countyName = in.readUTF();
        issuingAgency = in.readUTF();
    }

    @Override
    public String toString() {
        return "TicketNyc{" +
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

    public LocalDate getIssueDate() {
        return issueDate;
    }

    public Integer getCode() {
        return code;
    }

    public Double getFineAmount() {
        return fineAmount;
    }

    public String getCountyName() {
        return countyName;
    }

    public String getIssuingAgency() {
        return issuingAgency;
    }
}

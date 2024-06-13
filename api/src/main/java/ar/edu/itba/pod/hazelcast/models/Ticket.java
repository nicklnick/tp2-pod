package ar.edu.itba.pod.hazelcast.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Ticket implements DataSerializable {
    private LocalDateTime issueDate;
    private String plate;
    private String code;
    private String issuingAgency;
    private Double fineAmount;
    private String countyName;

    public Ticket() {
        // for Hazelcast
    }

    public Ticket(LocalDateTime issueDate,
                 String plate,
                 String code,
                 String issuingAgency,
                 Double fineAmount,
                 String countyName
    ) {
        this.issueDate = issueDate;
        this.plate = plate;
        this.code = code;
        this.issuingAgency = issuingAgency;
        this.fineAmount = fineAmount;
        this.countyName = countyName;
    }

    public static Ticket fromTicketChiCsv(String[] line) {
        return new Ticket(
                LocalDateTime.parse(line[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                line[1],
                line[2],
                line[3],
                Double.parseDouble(line[4]),
                line[5]
        );
    }

    public static Ticket fromTicketNycCsv(String[] line) {
        return new Ticket(
                LocalDate.parse(line[1], DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay(),
                line[0],
                line[2],
                line[5],
                Double.parseDouble(line[3]),
                line[4]
        );
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(issueDate.toString());
        out.writeUTF(plate);
        out.writeUTF(code);
        out.writeUTF(issuingAgency);
        out.writeDouble(fineAmount);
        out.writeUTF(countyName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        issueDate = LocalDateTime.parse(in.readUTF());
        plate = in.readUTF();
        code = in.readUTF();
        issuingAgency = in.readUTF();
        fineAmount = in.readDouble();
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

    public LocalDateTime getIssueDate() {
        return issueDate;
    }

    public String getCode() {
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

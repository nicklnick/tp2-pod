package ar.edu.itba.pod.hazelcast.models;

import ar.edu.itba.pod.hazelcast.util.CustomDateTimeFormatter;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

public class TicketChi implements DataSerializable {
    private UUID plate;
    private LocalDateTime issueDate;
    private String code;
    private Integer fineAmount;
    private String countyName;
    private String issuingAgency;

    public TicketChi() {
        // for Hazelcast
    }

    public TicketChi(UUID plate,
                     LocalDateTime issueDate,
                     String code,
                     Integer fineAmount,
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

    public static TicketChi fromTicketChiCsv(String[] line) {
        return new TicketChi(
                UUID.fromString(line[0]),
                LocalDateTime.parse(line[1], CustomDateTimeFormatter.YYYY_MM_DD_HH_MM_SS_WITH_HYPHEN),
                line[2],
                Integer.parseInt(line[3]),
                line[4],
                line[5]
        );
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(plate.getMostSignificantBits());
        out.writeLong(plate.getLeastSignificantBits());

        out.writeLong(issueDate.toEpochSecond(ZoneOffset.UTC));
        out.writeUTF(code);
        out.writeDouble(fineAmount);
        out.writeUTF(countyName);
        out.writeUTF(issuingAgency);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        long msb = in.readLong();
        long lsb = in.readLong();
        plate = new UUID(msb, lsb);

        issueDate = LocalDateTime.ofEpochSecond(in.readLong(), 0, ZoneOffset.UTC);
        code = in.readUTF();
        fineAmount = in.readInt();
        countyName = in.readUTF();
        issuingAgency = in.readUTF();
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

    public UUID getPlate() {
        return plate;
    }

    public LocalDateTime getIssueDate() {
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

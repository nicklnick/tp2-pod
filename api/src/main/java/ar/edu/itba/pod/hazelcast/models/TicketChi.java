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
    private LocalDateTime issueDate;
    private UUID plate;
    private String code;
    private String issuingAgency;
    private Integer fineAmount;
    private String countyName;

    public TicketChi() {
        // for Hazelcast
    }

    public TicketChi(LocalDateTime issueDate,
                     UUID plate,
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
                LocalDateTime.parse(line[0], CustomDateTimeFormatter.YYYY_MM_DD_HH_MM_SS_WITH_SLASH),
                UUID.fromString(line[1]),
                line[2],
                line[3],
                Integer.parseInt(line[4]),
                line[5]
        );
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(issueDate.toEpochSecond(ZoneOffset.UTC));

        out.writeLong(plate.getMostSignificantBits());
        out.writeLong(plate.getLeastSignificantBits());

        out.writeUTF(code);
        out.writeUTF(issuingAgency);
        out.writeInt(fineAmount);
        out.writeUTF(countyName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        issueDate = LocalDateTime.ofEpochSecond(in.readLong(), 0, ZoneOffset.UTC);

        long msb = in.readLong();
        long lsb = in.readLong();
        plate = new UUID(msb, lsb);

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

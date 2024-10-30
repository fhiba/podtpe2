package ar.edu.itba.pod.models;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;

public class Ticket implements DataSerializable, Serializable {
    private String plate;
    private int infractionId;
    private Double fineAmount;
    private String issuingAgency;
    private LocalDateTime issueDate;
    private String countyName;

    public Ticket(String plate, int infractionId, Double fineAmount, String issuingAgency, LocalDateTime issueDate, String countyName) {
        this.plate = plate;
        this.infractionId = infractionId;
        this.fineAmount = fineAmount;
        this.issuingAgency = issuingAgency;
        this.issueDate = issueDate;
        this.countyName = countyName;
    }
    public Ticket() {
        // Constructor sin argumentos necesario para la deserialización
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(plate);
        out.writeInt(infractionId);
        out.writeDouble(fineAmount);
        out.writeUTF(issuingAgency);
        out.writeObject(issueDate);
        out.writeUTF(countyName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        plate = in.readUTF();
        infractionId = in.readInt();
        fineAmount = in.readDouble();
        issuingAgency = in.readUTF();
        issueDate = in.readObject();
        countyName = in.readUTF();
    }

    public String getPlate() {
        return plate;
    }

    public Integer getInfractionId() {
        return infractionId;
    }

    public Double getFineAmount() {
        return fineAmount;
    }

    public String getIssuingAgency() {
        return issuingAgency;
    }

    public LocalDateTime getIssueDate() {
        return issueDate;
    }

    public String getCountyName() {
        return countyName;
    }
}

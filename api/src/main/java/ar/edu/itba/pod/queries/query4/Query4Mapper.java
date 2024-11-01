package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Context;
import java.io.Serializable;

public class Query4Mapper implements Mapper<String, Ticket, String, Double>, Serializable {

    private String agency;

    public Query4Mapper(String agency) {
        this.agency = agency;
    }

    @Override
    public void map(String key, Ticket ticket, Context<String, Double> context) {
        if (ticket.getIssuingAgency().equals(agency)) {
            String infractionCode = ticket.getInfractionId();
            Double fineAmount = ticket.getFineAmount();
            context.emit(infractionCode, fineAmount);
        }
    }
}
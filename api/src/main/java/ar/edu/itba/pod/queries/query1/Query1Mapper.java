package ar.edu.itba.pod.queries.query1;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;
import java.util.Set;

public class Query1Mapper implements Mapper<String, Ticket, String, Integer> {

    @Override
    public void map(String key, Ticket ticket, Context<String, Integer> context) {
        String infractionDescription = ticket.getInfractionDescription();
        String agency = ticket.getIssuingAgency();

        context.emit(infractionDescription + ";" + agency, 1);
    }
}
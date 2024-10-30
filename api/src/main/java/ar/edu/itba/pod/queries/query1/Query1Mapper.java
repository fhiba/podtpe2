package ar.edu.itba.pod.queries.query1;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;
import java.util.Set;

public class Query1Mapper implements Mapper<String, Ticket, String, Integer> {

    private Map<Integer, Infraction> infractionsMap;
    private Set<String> validAgencies;

    public Query1Mapper(Map<Integer, Infraction> infractionsMap, Set<String> validAgencies) {
        this.infractionsMap = infractionsMap;
        this.validAgencies = validAgencies;
    }

    @Override
    public void map(String key, Ticket ticket, Context<String, Integer> context) {
        String agency = ticket.getIssuingAgency();
        Integer infractionId = ticket.getInfractionId();

        // Verificar si la agencia y la infracción son válidas
        if (validAgencies.contains(agency) && infractionsMap.containsKey(infractionId)) {
            String infractionDefinition = infractionsMap.get(infractionId).getDefinition();
            String compositeKey = infractionDefinition + ";" + agency;
            context.emit(compositeKey, 1);
        }
    }
}
package ar.edu.itba.pod.queries.query1;

import com.hazelcast.mapreduce.Collator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Query1Collator implements Collator<Map.Entry<String, Integer>, List<Query1Result>>, Serializable {

    @Override
    public List<Query1Result> collate(Iterable<Map.Entry<String, Integer>> values) {
        List<Query1Result> resultList = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : values) {
            String compositeKey = entry.getKey();
            Integer ticketsCount = entry.getValue();

            // Separar la clave compuesta en InfractionDefinition y Agency
            String[] keyParts = compositeKey.split(";");
            if (keyParts.length != 2) {
                continue; // Saltar entradas inválidas
            }
            String infractionDefinition = keyParts[0];
            String agency = keyParts[1];

            resultList.add(new Query1Result(
                    infractionDefinition,
                    agency,
                    ticketsCount
            ));
        }

        // Ordenar los resultados según las especificaciones
        resultList.sort((a, b) -> {
            int cmp = b.getTickets().compareTo(a.getTickets()); // Descendente por cantidad de multas
            if (cmp != 0) return cmp;
            cmp = a.getInfraction().compareTo(b.getInfraction()); // Alfabético por infracción
            if (cmp != 0) return cmp;
            return a.getAgency().compareTo(b.getAgency()); // Alfabético por agencia
        });

        return resultList;
    }
}

package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.models.Infraction;
import com.hazelcast.mapreduce.Collator;
import java.util.*;

public class Query4Collator implements Collator<Map.Entry<String, MinMaxPair>, List<String>> {

    private int n;
    private Map<String, Infraction> infractionsMap;

    public Query4Collator(int n, Map<String, Infraction> infractionsMap) {
        this.n = n;
        this.infractionsMap = infractionsMap;
    }

    @Override
    public List<String> collate(Iterable<Map.Entry<String, MinMaxPair>> values) {
        List<InfractionDiff> infractionDiffs = new ArrayList<>();

        for (Map.Entry<String, MinMaxPair> entry : values) {
            String infractionCode = entry.getKey();

            // Only include infractions present in the infractions CSV
            if (infractionsMap.containsKey(infractionCode)) {
                MinMaxPair minMaxPair = entry.getValue();
                double min = minMaxPair.getMin();
                double max = minMaxPair.getMax();
                double diff = max - min;

                // Prepare InfractionDiff object
                Infraction infraction = infractionsMap.get(infractionCode);
                String infractionDescription = infraction.getDefinition();

                InfractionDiff infractionDiff = new InfractionDiff(
                        infractionDescription, min, max, diff);

                infractionDiffs.add(infractionDiff);
            }
        }

        // Sort the list as per requirements
        infractionDiffs.sort((o1, o2) -> {
            int cmp = Double.compare(o2.getDiff(), o1.getDiff()); // Descending by Diff
            if (cmp == 0) {
                cmp = o1.getInfraction().compareTo(o2.getInfraction()); // Alphabetical by Infraction
            }
            return cmp;
        });

        // Select top N infractions
        List<String> resultList = new ArrayList<>();
        int count = 0;
        for (InfractionDiff infractionDiff : infractionDiffs) {
            if (count >= n) break;
            String line = String.format("%s;%.0f;%.0f;%.0f",
                    infractionDiff.getInfraction(),
                    infractionDiff.getMax(),
                    infractionDiff.getMin(),
                    infractionDiff.getDiff());
            resultList.add(line);
            count++;
        }

        return resultList;
    }
}

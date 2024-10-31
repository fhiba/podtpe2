package ar.edu.itba.pod.queries.query3;

import com.hazelcast.mapreduce.Collator;
import java.util.*;

public class Query3Collator implements Collator<Map.Entry<String, Boolean>, List<String>> {

    @Override
    public List<String> collate(Iterable<Map.Entry<String, Boolean>> values) {
        Map<String, Integer> totalPlatesPerCounty = new HashMap<>();
        Map<String, Integer> repeatOffendersPerCounty = new HashMap<>();

        for (Map.Entry<String, Boolean> entry : values) {
            String key = entry.getKey();
            Boolean isRepeatOffender = entry.getValue();

            String[] parts = key.split(";");
            String county = parts[0];

            totalPlatesPerCounty.merge(county, 1, Integer::sum);
            if (isRepeatOffender) {
                repeatOffendersPerCounty.merge(county, 1, Integer::sum);
            }
        }

        // Compute percentages and prepare output
        List<Map.Entry<String, Double>> countyPercentages = new ArrayList<>();
        for (String county : totalPlatesPerCounty.keySet()) {
            int totalPlates = totalPlatesPerCounty.get(county);
            int repeatOffenders = repeatOffendersPerCounty.getOrDefault(county, 0);

            double percentage = ((double) repeatOffenders / totalPlates) * 100.0;
            percentage = Math.floor(percentage * 100.0) / 100.0; // Truncate to two decimals

            countyPercentages.add(new AbstractMap.SimpleEntry<>(county, percentage));
        }

        // Sort as per requirements
        countyPercentages.sort((e1, e2) -> {
            int cmp = Double.compare(e2.getValue(), e1.getValue()); // Descending by percentage
            if (cmp == 0) {
                cmp = e1.getKey().compareTo(e2.getKey()); // Alphabetical by county
            }
            return cmp;
        });

        // Prepare output lines
        List<String> resultList = new ArrayList<>();
        for (Map.Entry<String, Double> entry : countyPercentages) {
            String county = entry.getKey();
            double percentage = entry.getValue();
            resultList.add(county + ";" + String.format("%.2f%%", percentage));
        }

        return resultList;
    }
}
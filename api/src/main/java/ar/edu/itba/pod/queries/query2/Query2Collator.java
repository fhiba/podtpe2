package ar.edu.itba.pod.queries.query2;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query2Collator implements Collator<Map.Entry<String, Double>, List<String>> {

    @Override
    public List<String> collate(Iterable<Map.Entry<String, Double>> values) {
        // Map to hold data: Map<Agency, Map<Year, Map<Month, Revenue>>>
        Map<String, Map<Integer, Map<Integer, Double>>> agencyYearMonthRevenueMap = new HashMap<>();

        for (Map.Entry<String, Double> entry : values) {
            String key = entry.getKey();
            Double revenue = entry.getValue();

            String[] parts = key.split(";");
            String agency = parts[0];
            int year = Integer.parseInt(parts[1]);
            int month = Integer.parseInt(parts[2]);

            agencyYearMonthRevenueMap
                    .computeIfAbsent(agency, k -> new HashMap<>())
                    .computeIfAbsent(year, k -> new HashMap<>())
                    .put(month, revenue);
        }

        // Prepare the result list
        List<String> resultList = new ArrayList<>();

        // Get list of agencies and sort alphabetically
        List<String> agencies = new ArrayList<>(agencyYearMonthRevenueMap.keySet());
        Collections.sort(agencies);

        // For each agency (sorted)
        for (String agency : agencies) {
            Map<Integer, Map<Integer, Double>> yearMonthRevenueMap = agencyYearMonthRevenueMap.get(agency);

            // Get list of years for this agency, sort them
            List<Integer> years = new ArrayList<>(yearMonthRevenueMap.keySet());
            Collections.sort(years);

            // For each year (sorted)
            for (Integer year : years) {
                Map<Integer, Double> monthRevenueMap = yearMonthRevenueMap.get(year);

                // Get list of months, sort them
                List<Integer> months = new ArrayList<>(monthRevenueMap.keySet());
                Collections.sort(months);

                // Compute YTD sums
                double ytd = 0.0;
                for (Integer month : months) {
                    double revenue = monthRevenueMap.get(month);
                    ytd += revenue;

                    // Only output months where revenue > 0
                    if (revenue > 0) {
                        String outputLine = agency + ";" + year + ";" + month + ";" + (int) ytd;
                        resultList.add(outputLine);
                    }
                }
            }
        }

        return resultList;
    }
}
package ar.edu.itba.pod.queries.query1;

import ar.edu.itba.pod.models.Infraction;
import com.hazelcast.mapreduce.Collator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.hazelcast.mapreduce.Collator;

public class Query1Collator implements Collator<Map.Entry<String, Integer>, List<Map.Entry<String, Integer>>> {

    @Override
    public List<Map.Entry<String, Integer>> collate(Iterable<Map.Entry<String, Integer>> values) {
        List<Map.Entry<String, Integer>> resultList = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : values) {
            resultList.add(entry);
        }

        // Sort results as per requirements
        Collections.sort(resultList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
                int cmp = e2.getValue().compareTo(e1.getValue()); // Descending by tickets
                if (cmp == 0) {
                    String[] split1 = e1.getKey().split(";");
                    String[] split2 = e2.getKey().split(";");
                    cmp = split1[0].compareTo(split2[0]); // Alphabetical by infraction
                    if (cmp == 0) {
                        cmp = split1[1].compareTo(split2[1]); // Alphabetical by agency
                    }
                }
                return cmp;
            }
        });

        return resultList;
    }
}
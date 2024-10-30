package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class WordCountCollator implements Collator<Map.Entry<String, Long>,
        List<Map.Entry<String, Long>>> {
    @Override
    public List<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                        .thenComparing((Map.Entry.<String, Long>comparingByKey())))
                .collect(Collectors.toList());
    }
}

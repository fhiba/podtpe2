package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.StringTokenizer;

public class LongestWordMapper implements Mapper<String, String, String, String> {
    @Override
    public void map(String s, String s2, Context<String, String> context) {
        StringTokenizer tokenizer = new StringTokenizer(s2.toLowerCase());
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            if(word.matches("[a-z]+"))
                context.emit(word.substring(0,1), word);
        }
    }
}

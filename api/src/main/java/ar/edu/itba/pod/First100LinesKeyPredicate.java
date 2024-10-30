package ar.edu.itba.pod;

import com.hazelcast.mapreduce.KeyPredicate;

public class First100LinesKeyPredicate implements KeyPredicate<String> {
    @Override
    public boolean evaluate(String key) {
        return Integer.parseInt(key) < 100;
    }
}
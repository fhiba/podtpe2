package ar.edu.itba.pod.queries.query4;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import java.io.Serializable;

public class Query4CombinerFactory implements CombinerFactory<String, Double, MinMaxPair>, Serializable {

    @Override
    public Combiner<Double, MinMaxPair> newCombiner(String key) {
        return new Combiner<Double, MinMaxPair>() {

            private double min = Double.MAX_VALUE;
            private double max = Double.MIN_VALUE;

            @Override
            public void combine(Double value) {
                min = Math.min(min, value);
                max = Math.max(max, value);
            }

            @Override
            public MinMaxPair finalizeChunk() {
                MinMaxPair result = new MinMaxPair(min, max);
                min = Double.MAX_VALUE;
                max = Double.MIN_VALUE;
                return result;
            }
        };
    }
}
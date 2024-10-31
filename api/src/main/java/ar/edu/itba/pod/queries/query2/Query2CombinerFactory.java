package ar.edu.itba.pod.queries.query2;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query2CombinerFactory implements CombinerFactory<String, Double, Double> {

    @Override
    public Combiner<Double, Double> newCombiner(String key) {
        return new Combiner<Double, Double>() {
            private double sum = 0.0;

            @Override
            public void combine(Double value) {
                sum += value;
            }

            @Override
            public Double finalizeChunk() {
                double result = sum;
                sum = 0.0;
                return result;
            }
        };
    }
}

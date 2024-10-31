package ar.edu.itba.pod.queries.query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2ReducerFactory implements ReducerFactory<String, Double, Double> {

    @Override
    public Reducer<Double, Double> newReducer(String key) {
        return new Reducer<Double, Double>() {
            private double sum = 0.0;

            @Override
            public void beginReduce() {
                sum = 0.0;
            }

            @Override
            public void reduce(Double value) {
                sum += value;
            }

            @Override
            public Double finalizeReduce() {
                return sum;
            }
        };
    }
}
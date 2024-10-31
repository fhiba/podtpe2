package ar.edu.itba.pod.queries.query1;

import ar.edu.itba.pod.models.Infraction;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query1ReducerFactory implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String key) {
        return new Reducer<Integer, Integer>() {

            private int sum;

            @Override
            public void beginReduce() {
                sum = 0;
            }

            @Override
            public void reduce(Integer value) {
                sum += value;
            }

            @Override
            public Integer finalizeReduce() {
                return sum;
            }
        };
    }
}

package ar.edu.itba.pod.queries.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.io.Serializable;

public class Query4ReducerFactory implements ReducerFactory<String, MinMaxPair, MinMaxPair>, Serializable {

    @Override
    public Reducer<MinMaxPair, MinMaxPair> newReducer(String key) {
        return new Reducer<MinMaxPair, MinMaxPair>() {

            private double min = Double.MAX_VALUE;
            private double max = Double.MIN_VALUE;

            @Override
            public void beginReduce() {
                // Initialization if needed
            }

            @Override
            public void reduce(MinMaxPair value) {
                min = Math.min(min, value.getMin());
                max = Math.max(max, value.getMax());
            }

            @Override
            public MinMaxPair finalizeReduce() {
                return new MinMaxPair(min, max);
            }
        };
    }
}
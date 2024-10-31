package ar.edu.itba.pod.queries.query3;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Query3ReducerFactory implements ReducerFactory<String, String, Boolean>, Serializable {

    private int n;

    public Query3ReducerFactory(int n) {
        this.n = n;
    }

    @Override
    public Reducer<String, Boolean> newReducer(String key) {
        return new Query3Reducer(n);
    }

    private class Query3Reducer extends Reducer<String, Boolean> {
        private int n;
        private Map<String, Integer> infractionCounts;

        public Query3Reducer(int n) {
            this.n = n;
        }

        @Override
        public void beginReduce() {
            infractionCounts = new HashMap<>();
        }

        @Override
        public void reduce(String infractionCode) {
            infractionCounts.merge(infractionCode, 1, Integer::sum);
        }

        @Override
        public Boolean finalizeReduce() {
            return infractionCounts.values().stream().anyMatch(count -> count >= n);
        }
    }
}

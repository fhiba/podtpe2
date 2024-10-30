package ar.edu.itba.pod.queries.query1;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query1CombinerFactory implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String key) {
        return new Query1Combiner();
    }

    private static class Query1Combiner extends Combiner<Integer, Integer> {
        private int sum = 0;

        @Override
        public void combine(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeChunk() {
            int result = sum;
            sum = 0; // Reiniciar el sumador para el próximo chunk
            return result;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }
}

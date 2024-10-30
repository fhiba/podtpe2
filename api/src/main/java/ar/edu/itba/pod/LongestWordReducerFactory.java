package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class LongestWordReducerFactory implements ReducerFactory<String, String, String> {
    @Override
    public Reducer<String, String> newReducer(String key) {
        return new LongestWordReducerFactory.LongestWordReducer();
    }

    private class LongestWordReducer extends Reducer<String, String> {
        String longestWord = "";
        @Override
        public void reduce(String value) {
            if(value.length() > longestWord.length())
                longestWord = value;
            else if(value.length() == longestWord.length() && value.compareTo(longestWord) < 0)
                longestWord = value;
        }

        @Override
        public String finalizeReduce() {
            return longestWord;
        }
    }
}

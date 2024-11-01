package ar.edu.itba.pod.queries.query4;

import java.io.Serializable;

public class MinMaxPair implements Serializable {
    private double min;
    private double max;

    public MinMaxPair(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }
}

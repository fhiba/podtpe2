package ar.edu.itba.pod.queries.query4;

public class InfractionDiff {
    private String infraction;
    private double min;
    private double max;
    private double diff;

    public InfractionDiff(String infraction, double min, double max, double diff) {
        this.infraction = infraction;
        this.min = min;
        this.max = max;
        this.diff = diff;
    }

    public String getInfraction() {
        return infraction;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getDiff() {
        return diff;
    }
}

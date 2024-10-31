package ar.edu.itba.pod.queries.query1;

public class Query1Result {
    private final String infraction;
    private final String agency;
    private final Integer tickets;

    public Query1Result(String infraction, String agency) {
        this.infraction = infraction;
        this.agency = agency;
        this.tickets = 0;
    }

    public String getInfraction() {
        return infraction;
    }

    public String getAgency() {
        return agency;
    }

    public Integer getTickets() {
        return tickets;
    }
}

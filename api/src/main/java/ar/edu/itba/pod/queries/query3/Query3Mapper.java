package ar.edu.itba.pod.queries.query3;

import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Query3Mapper implements Mapper<String, Ticket, String, String>, Serializable {

    private LocalDateTime fromDate;
    private LocalDateTime toDate;

    public Query3Mapper(LocalDateTime fromDate, LocalDateTime toDate) {
        this.fromDate = fromDate;
        this.toDate = toDate;
    }

    @Override
    public void map(String key, Ticket ticket, Context<String, String> context) {
        LocalDateTime issueDate = ticket.getIssueDate();
        if (!issueDate.isBefore(fromDate) && !issueDate.isAfter(toDate)) {
            String county = ticket.getCountyName();
            String plate = ticket.getPlate();
            String infractionCode = ticket.getInfractionId();

            String mapKey = county + ";" + plate;
            context.emit(mapKey, infractionCode);
        }
    }
}
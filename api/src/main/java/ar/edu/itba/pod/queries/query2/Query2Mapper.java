package ar.edu.itba.pod.queries.query2;

import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

public class Query2Mapper implements Mapper<String, Ticket, String, Double> {

    @Override
    public void map(String key, Ticket ticket, Context<String, Double> context) {
        String agency = ticket.getIssuingAgency();
        LocalDateTime issueDate = ticket.getIssueDate();
        int year = issueDate.getYear();
        int month = issueDate.getMonthValue();

        String mapKey = agency + ";" + year + ";" + month;
        Double fineAmount = ticket.getFineAmount();

        context.emit(mapKey, fineAmount);
    }
}
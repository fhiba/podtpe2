package ar.edu.itba.pod.client.adapters;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DataAdapter {
    List<Ticket> readTickets(String filePath) throws IOException;
    public Map<Integer, Infraction> readInfractions(String filePath) throws IOException;
    Set<String> readAgencies(String filePath) throws IOException;
}
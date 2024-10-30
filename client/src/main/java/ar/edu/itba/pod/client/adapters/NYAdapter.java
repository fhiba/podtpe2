package ar.edu.itba.pod.client.adapters;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class NYAdapter implements DataAdapter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public List<Ticket> readTickets(String filePath) throws IOException {
        List<Ticket> tickets = new ArrayList<>();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            String line = br.readLine(); // Leer encabezado y descartarlo

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(";");
                if (fields.length < 6) {
                    continue; // Saltar líneas mal formateadas
                }

                // Parsear y mapear los campos
                String plate = fields[0];
                String infractionIdStr = fields[1];
                String fineAmountStr = fields[2];
                String issuingAgency = fields[3];
                String issueDateStr = fields[4];
                String countyName = fields[5];

                try {
                    Integer infractionId = Integer.parseInt(infractionIdStr);
                    Double fineAmount = Double.parseDouble(fineAmountStr);
                    LocalDate issueDate = LocalDate.parse(issueDateStr, DATE_FORMATTER);
                    LocalDateTime issueDateTime = LocalDateTime.of(issueDate, LocalTime.MIDNIGHT);

                    // Crear instancia de Ticket
                    Ticket ticket = new Ticket(
                            plate,
                            infractionId,
                            fineAmount,
                            issuingAgency,
                            issueDateTime,
                            countyName
                    );

                    tickets.add(ticket);
                } catch (NumberFormatException | DateTimeParseException e) {
                    // Saltar líneas con datos inválidos
                    continue;
                }
            }
        }

        return tickets;
    }

    @Override
    public Map<Integer, Infraction> readInfractions(String filePath) throws IOException {
        Map<Integer, Infraction> infractions = new HashMap<>();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            String line = br.readLine(); // Leer encabezado y descartarlo

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(";");
                if (fields.length < 2) {
                    continue; // Saltar líneas mal formateadas
                }

                String infractionIdStr = fields[0];
                String infractionDefinition = fields[1];

                try {
                    Integer infractionId = Integer.parseInt(infractionIdStr);
                    Infraction infraction = new Infraction(infractionId.toString(), infractionDefinition);
                    infractions.put(infractionId, infraction);
                } catch (NumberFormatException e) {
                    // Saltar líneas con datos inválidos
                    continue;
                }
            }
        }

        return infractions;
    }

    @Override
    public Set<String> readAgencies(String filePath) throws IOException {
        Set<String> agencies = new HashSet<>();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            String line = br.readLine(); // Leer encabezado y descartarlo

            while ((line = br.readLine()) != null) {
                String agencyName = line.trim();
                if (!agencyName.isEmpty()) {
                    agencies.add(agencyName);
                }
            }
        }

        return agencies;
    }
}
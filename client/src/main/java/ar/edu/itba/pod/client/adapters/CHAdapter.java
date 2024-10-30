package ar.edu.itba.pod.client.adapters;
import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CHAdapter implements DataAdapter {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Mapa para asignar IDs únicos a los códigos de infracción
    private final Map<String, Integer> infractionCodeMap = new HashMap<>();
    private int infractionIdCounter = 1;

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
                String issueDateStr = fields[0];
                String countyName = fields[1];
                String issuingAgency = fields[2];
                String plate = fields[3];
                String infractionCodeStr = fields[4];
                String fineAmountStr = fields[5];

                LocalDateTime issueDate = parseIssueDate(issueDateStr);
                Integer infractionId = getInfractionId(infractionCodeStr);
                Double fineAmount = parseFineAmount(fineAmountStr);

                // Crear instancia de Ticket
                Ticket ticket = new Ticket(
                        plate,
                        infractionId,
                        fineAmount,
                        issuingAgency,
                        issueDate,
                        countyName
                );

                tickets.add(ticket);
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

                String infractionCodeStr = fields[0];
                String infractionDescription = fields[1];

                Integer infractionId = getInfractionId(infractionCodeStr);
                Infraction infraction = new Infraction(infractionCodeStr, infractionDescription);
                infractions.put(infractionId, infraction);
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

    // Métodos auxiliares

    private LocalDateTime parseIssueDate(String dateStr) {
        return LocalDateTime.parse(dateStr, DATE_TIME_FORMATTER);
    }

    private Integer getInfractionId(String codeStr) {
        // Asignar un ID único a cada código de infracción
        if (!infractionCodeMap.containsKey(codeStr)) {
            infractionCodeMap.put(codeStr, infractionIdCounter++);
        }
        return infractionCodeMap.get(codeStr);
    }

    private Double parseFineAmount(String amountStr) {
        return Double.parseDouble(amountStr);
    }
}

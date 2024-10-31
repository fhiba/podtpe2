package ar.edu.itba.pod.client.queries;
import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Utils {

    public static HazelcastInstance getClient(String addresses){
        // Split addresses
        String[] addressArray = addresses.split(";");
        // Configure the client
        ClientConfig clientConfig = new ClientConfig();
        GroupConfig groupConfig = new GroupConfig()
                .setName("g12")
                .setPassword("g12-pass");
        clientConfig.setGroupConfig(groupConfig);
        clientConfig.getNetworkConfig().addAddress(addressArray);
        return HazelcastClient.newHazelcastClient(clientConfig);
    }
    public static String getCurrentTimestamp() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss:SSSS");
        return LocalDateTime.now().format(formatter);
    }

    public static void logTime(String message, String timeFilePath) throws IOException {
        String timestamp = getCurrentTimestamp();
        String logMessage = timestamp + " INFO  [main] Client - " + message;
        // Append the log message to the time file
        try (FileWriter fw = new FileWriter(timeFilePath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(logMessage);
        }
    }

    public static void loadInfractions(String filePath, IMap<String, Infraction> infractionsMap) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(";");
                if (tokens.length >= 2) {
                    String code = tokens[0];
                    String description = tokens[1];
                    infractionsMap.put(code, new Infraction(code, description));
                }
            }
        }
    }

    public static void loadAgencies(String filePath, IMap<String, String> agenciesMap) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String agencyName = line.trim();
                agenciesMap.put(agencyName, agencyName);
            }
        }
    }

    public static void loadTickets(String filePath, String city, IMap<String, Infraction> infractionsMap, IMap<String, String> agenciesMap, IMap<String, Ticket> ticketsMap) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine(); // Skip header
            int count = 0;
            DateTimeFormatter formatterCHI = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            DateTimeFormatter formatterNYC = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(";");
                if (city.equalsIgnoreCase("CHI")) {
                    if (tokens.length >= 6) {
                        String issueDateStr = tokens[0];
                        String communityAreaName = tokens[1];
                        String unitDescription = tokens[2];
                        String licensePlateNumber = tokens[3];
                        String violationCode = tokens[4];
                        String fineAmountStr = tokens[5];

                        // Check if infraction and agency are valid
                        if (infractionsMap.containsKey(violationCode) && agenciesMap.containsKey(unitDescription)) {
                            LocalDateTime issueDate = LocalDateTime.parse(issueDateStr, formatterCHI);
                            Double fineAmount = Double.parseDouble(fineAmountStr);

                            Infraction infraction = infractionsMap.get(violationCode);
                            String infractionDescription = infraction.getDefinition();

                            Ticket ticket = new Ticket(
                                    licensePlateNumber,
                                    violationCode,
                                    infractionDescription,
                                    fineAmount,
                                    unitDescription,
                                    issueDate,
                                    communityAreaName
                            );
                            ticketsMap.put("ticket" + count, ticket);
                            count++;
                        }
                    }
                } else if (city.equalsIgnoreCase("NYC")) {
                    if (tokens.length >= 6) {
                        String plate = tokens[0];
                        String infractionId = tokens[1];
                        String fineAmountStr = tokens[2];
                        String issuingAgency = tokens[3];
                        String issueDateStr = tokens[4];
                        String countyName = tokens[5];

                        // Check if infraction and agency are valid
                        if (infractionsMap.containsKey(infractionId) && agenciesMap.containsKey(issuingAgency)) {
                            LocalDateTime issueDate = LocalDate.parse(issueDateStr, formatterNYC).atStartOfDay();
                            Double fineAmount = Double.parseDouble(fineAmountStr);

                            Infraction infraction = infractionsMap.get(infractionId);
                            String infractionDescription = infraction.getDefinition();

                            Ticket ticket = new Ticket(
                                    plate,
                                    infractionId,
                                    infractionDescription,
                                    fineAmount,
                                    issuingAgency,
                                    issueDate,
                                    countyName
                            );
                            ticketsMap.put("ticket" + count, ticket);
                            count++;
                        }
                    }
                }
            }
        }
    }
}

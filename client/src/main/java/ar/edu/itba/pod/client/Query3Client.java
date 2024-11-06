package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.queries.query3.Query3Collator;
import ar.edu.itba.pod.queries.query3.Query3Mapper;
import ar.edu.itba.pod.queries.query3.Query3ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static ar.edu.itba.pod.client.Utils.logTime;

public class Query3Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(Query3Client.class);
    public static void main(String[] args) throws Exception {
        // Retrieve parameters
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        String nStr = System.getProperty("n");
        String fromStr = System.getProperty("from");
        String toStr = System.getProperty("to");

        if (addresses == null || city == null || inPath == null || outPath == null) {
            System.err.println("Missing required parameters.");
            System.exit(1);
        }
        int n = Integer.parseInt(nStr);
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        LocalDateTime fromDate = LocalDate.parse(fromStr, dateFormatter).atStartOfDay();
        LocalDateTime toDate = LocalDate.parse(toStr, dateFormatter).atTime(23, 59, 59);

        // Create a Hazelcast client instance
        HazelcastInstance client = Utils.getClient(addresses);

        // Determine file names based on city
        String ticketsFileName = "tickets" + city + ".csv";
        String infractionsFileName = "infractions" + city + ".csv";
        String agenciesFileName = "agencies" + city + ".csv";

        // Build full paths
        String ticketsFilePath = inPath + File.separator + ticketsFileName;
        String infractionsFilePath = inPath + File.separator + infractionsFileName;
        String agenciesFilePath = inPath + File.separator + agenciesFileName;
        // Output files
        String outputFilePath = outPath + File.separator + "query3.csv";
        String timeFilePath = outPath + File.separator + "time3.txt";

        // Delete time3.txt if it exists
        File timeFile = new File(timeFilePath);
        if (timeFile.exists()) {
            timeFile.delete();
        }
        // Delete query3.csv if it exists
        File outputFile = new File(outputFilePath);
        if (outputFile.exists()) {
            outputFile.delete();
        }

        // Get references to distributed maps
        IMap<String, Infraction> infractionsMap = client.getMap("infractions");
        IMap<String, String> agenciesMap = client.getMap("agencies");
        IMap<String, Ticket> ticketsMap = client.getMap("tickets");

        // Record start of reading input files
        logTime("Inicio de la lectura del archivo", timeFilePath);

        // Load data into maps
        Utils.loadInfractions(infractionsFilePath, infractionsMap);
        Utils.loadAgencies(agenciesFilePath, agenciesMap);
        Utils.loadTickets(ticketsFilePath, city, infractionsMap, agenciesMap, ticketsMap);

        // Record end of reading input files
        logTime("Fin de lectura del archivo", timeFilePath);

        // Record start of MapReduce job
        logTime("Inicio del trabajo map/reduce", timeFilePath);

        // Execute MapReduce job
        JobTracker jobTracker = client.getJobTracker("default");
        Job<String, Ticket> job = jobTracker.newJob(KeyValueSource.fromMap(ticketsMap));

        Query3Mapper mapper = new Query3Mapper(fromDate, toDate);
        Query3ReducerFactory reducerFactory = new Query3ReducerFactory(n);

        JobCompletableFuture<List<String>> future = job
                .mapper(mapper)
                .reducer(reducerFactory)
                .submit(new Query3Collator());

        // Collect results
        List<String> resultList = future.get();

        // Record end of MapReduce job (includes writing output)
        logTime("Fin del trabajo map/reduce", timeFilePath);

        // Output the results to query3.csv
        try (PrintWriter writer = new PrintWriter(new File(outputFilePath))) {
            writer.println("County;Percentage");
            for (String line : resultList) {
                writer.println(line);
            }
        }

        // Shutdown client
        client.shutdown();
    }
}

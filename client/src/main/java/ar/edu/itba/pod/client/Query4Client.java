package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.queries.query4.Query4Collator;
import ar.edu.itba.pod.queries.query4.Query4CombinerFactory;
import ar.edu.itba.pod.queries.query4.Query4Mapper;
import ar.edu.itba.pod.queries.query4.Query4ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.File;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static ar.edu.itba.pod.client.Utils.logTime;

public class Query4Client {
    public static void main(String[] args) throws Exception {
        // Retrieve parameters
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        String nStr = System.getProperty("n");
        String agencyParam = System.getProperty("agency"); // Note: 'agency' not 'Dagency'


        if (addresses == null || city == null || inPath == null || outPath == null) {
            System.err.println("Missing required parameters.");
            System.exit(1);
        }
        int n = Integer.parseInt(nStr);
        // Replace underscores with spaces in agency name
        String agency = agencyParam.replace("_", " ");
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
        String outputFilePath = outPath + File.separator + "query4.csv";
        String timeFilePath = outPath + File.separator + "time4.txt";

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

        Query4Mapper mapper = new Query4Mapper(agency);

        // Collect infractions map for Collator
        Map<String, Infraction> infractionsLocalMap = infractionsMap.getAll(infractionsMap.keySet());

        JobCompletableFuture<List<String>> future = job
                .mapper(mapper)
                .combiner(new Query4CombinerFactory())
                .reducer(new Query4ReducerFactory())
                .submit(new Query4Collator(n, infractionsLocalMap));

        // Collect results
        List<String> resultList = future.get();

        // Record end of MapReduce job (includes writing output)
        logTime("Fin del trabajo map/reduce", timeFilePath);

        // Output the results to query4.csv
        try (PrintWriter writer = new PrintWriter(new File(outputFilePath))) {
            writer.println("Infraction;Max;Min;Diff");
            for (String line : resultList) {
                writer.println(line);
            }
        }

        // Shutdown client
        client.shutdown();
    }
}

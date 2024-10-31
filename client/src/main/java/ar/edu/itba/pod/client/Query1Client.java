package ar.edu.itba.pod.client;
import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.queries.query1.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import ar.edu.itba.pod.models.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.*;

public class Query1Client{
    private static final Logger LOGGER = LoggerFactory.getLogger(Query1Client.class);

    public static void main(String[] args) throws Exception {
        // Retrieve parameters
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");

        if (addresses == null || city == null || inPath == null || outPath == null) {
            System.err.println("Missing required parameters.");
            System.exit(1);
        }

        // Create a Hazelcast client instance
        HazelcastInstance client = Utils.getClient(addresses);

        // Determine file names based on city
        String ticketsFileName = "tickets" + city + "100k.csv";
        String infractionsFileName = "infractions" + city + ".csv";
        String agenciesFileName = "agencies" + city + ".csv";

        // Build full paths
        String ticketsFilePath = inPath + File.separator + ticketsFileName;
        String infractionsFilePath = inPath + File.separator + infractionsFileName;
        String agenciesFilePath = inPath + File.separator + agenciesFileName;

        // Output files
        String outputFilePath = outPath + File.separator + "query1.csv";
        String timeFilePath = outPath + File.separator + "time1.txt";


        // Get references to distributed maps
        IMap<String, Infraction> infractionsMap = client.getMap("infractions");
        IMap<String, String> agenciesMap = client.getMap("agencies");
        IMap<String, Ticket> ticketsMap = client.getMap("tickets");

        // Record start of reading input files
        Utils.logTime("Inicio de la lectura del archivo", timeFilePath);
        // Load data into maps
        Utils.loadInfractions(infractionsFilePath, infractionsMap);
        Utils.loadAgencies(agenciesFilePath, agenciesMap);
        Utils.loadTickets(ticketsFilePath, city, infractionsMap, agenciesMap, ticketsMap);
        // Record end of reading input files
        Utils.logTime("Fin de lectura del archivo", timeFilePath);

        // Record start of MapReduce job
        Utils.logTime("Inicio del trabajo map/reduce", timeFilePath);
        // Execute MapReduce job
        JobTracker jobTracker = client.getJobTracker("default");
        Job<String, Ticket> job = jobTracker.newJob(KeyValueSource.fromMap(ticketsMap));

        JobCompletableFuture<List<Map.Entry<String, Integer>>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new Query1ReducerFactory())
                .submit(new Query1Collator());
        // Collect results
        List<Map.Entry<String, Integer>> resultList = future.get();

        // Record end of MapReduce job (includes writing output)
        Utils.logTime("Fin del trabajo map/reduce", timeFilePath);

        // Output the results to query1.csv
        try (PrintWriter writer = new PrintWriter(outputFilePath)) {
            writer.println("Infraction;Agency;Tickets");
            for (Map.Entry<String, Integer> entry : resultList) {
                String key = entry.getKey();
                Integer count = entry.getValue();
                writer.println(key + ";" + count);
            }
        }
        // Shutdown client
        client.shutdown();
    }
}
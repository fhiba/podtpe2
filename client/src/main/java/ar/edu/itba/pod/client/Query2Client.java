package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.queries.query2.Query2Collator;
import ar.edu.itba.pod.queries.query2.Query2CombinerFactory;
import ar.edu.itba.pod.queries.query2.Query2Mapper;
import ar.edu.itba.pod.queries.query2.Query2ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

public class Query2Client {

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
        String outputFilePath = outPath + File.separator + "query2.csv";
        String timeFilePath = outPath + File.separator + "time2.txt";

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

        // Execute MapReduce job with Combiner and Collator
        JobTracker jobTracker = client.getJobTracker("default");
        Job<String, Ticket> job = jobTracker.newJob(KeyValueSource.fromMap(ticketsMap));

        JobCompletableFuture<List<String>> future = job
                .mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory())
                .reducer(new Query2ReducerFactory())
                .submit(new Query2Collator());

        // Collect results
        List<String> resultList = future.get();

        // Record end of MapReduce job (includes writing output)
        Utils.logTime("Fin del trabajo map/reduce", timeFilePath);

        // Output the results to query2.csv
        try (PrintWriter writer = new PrintWriter(outputFilePath)) {
            writer.println("Agency;Year;Month;YTD");
            for (String line : resultList) {
                writer.println(line);
            }
        }

        // Shutdown client
        client.shutdown();
    }
}

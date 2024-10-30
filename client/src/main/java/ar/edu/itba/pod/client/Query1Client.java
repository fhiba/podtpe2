package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.adapters.CHAdapter;
import ar.edu.itba.pod.client.adapters.DataAdapter;
import ar.edu.itba.pod.client.adapters.NYAdapter;
import ar.edu.itba.pod.client.utils.TimestampWriter;
import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.queries.query1.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query1Client {

    public static void main(String[] args) {
        new Query1Client().run();
    }

    public void run() {
        // Parsear argumentos
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");

        // Validar parámetros requeridos
        if (addresses == null || city == null || inPath == null || outPath == null) {
            System.err.println("Faltan parámetros requeridos.");
            System.err.println("Uso: -Daddresses='...' -Dcity=... -DinPath=... -DoutPath=...");
            System.exit(1);
        }

        HazelcastInstance client = null;
        try {
            // Inicializar cliente Hazelcast
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getNetworkConfig().addAddress(addresses.split(";"));
            clientConfig.getGroupConfig().setName("g12").setPassword("g12-pass"); // Reemplaza 'g12' con tu número de grupo

            client = HazelcastClient.newHazelcastClient(clientConfig);

            // Registrar inicio de lectura
            LocalDateTime startReadTime = LocalDateTime.now();
            TimestampWriter.writeTimestamp(outPath + "/time1.txt", "Inicio de la lectura de los archivos", startReadTime);

            // Cargar datos utilizando el adaptador adecuado
            IList<Ticket> tickets = client.getList("g12-tickets"); // Reemplaza 'g12' con tu número de grupo
            DataAdapter adapter;
            if ("NYC".equalsIgnoreCase(city)) {
                adapter = new NYAdapter();
            } else if ("CHI".equalsIgnoreCase(city)) {
                adapter = new CHAdapter();
            } else {
                System.err.println("Ciudad no soportada: " + city);
                System.exit(1);
                return;
            }

            // Leer tickets y agregar a la lista de Hazelcast
            List<Ticket> ticketList = adapter.readTickets(inPath + "/tickets" + city + ".csv");
            tickets.clear(); // Limpiar datos existentes si los hay
            tickets.addAll(ticketList);

            // Leer infracciones y agencias
            Map<Integer, Infraction> infractionsMap = adapter.readInfractions(inPath + "/infractions" + city + ".csv");
            Set<String> validAgencies = adapter.readAgencies(inPath + "/agencies" + city + ".csv");

            // Registrar fin de lectura
            LocalDateTime endReadTime = LocalDateTime.now();
            TimestampWriter.writeTimestamp(outPath + "/time1.txt", "Fin de lectura de los archivos", endReadTime);

            // Configurar y ejecutar el trabajo MapReduce
            JobTracker jobTracker = client.getJobTracker("g12-job-tracker"); // Reemplaza 'g12' con tu número de grupo
            KeyValueSource<String, Ticket> source = KeyValueSource.fromList(tickets);

            // Registrar inicio del MapReduce
            LocalDateTime startMapReduceTime = LocalDateTime.now();
            TimestampWriter.writeTimestamp(outPath + "/time1.txt", "Inicio del trabajo MapReduce", startMapReduceTime);

            // Ejecutar MapReduce con Combiner y Collator
            Job<String, Ticket> job = jobTracker.newJob(source);
            List<Query1Result> resultList = job.mapper(
                    new Query1Mapper(infractionsMap, validAgencies)
            ).combiner(
                    new Query1CombinerFactory()
            ).reducer(
                    new Query1ReducerFactory()
            ).submit(
                    new Query1Collator()
            ).get();

            // Registrar fin del MapReduce
            LocalDateTime endMapReduceTime = LocalDateTime.now();
            TimestampWriter.writeTimestamp(outPath + "/time1.txt", "Fin del trabajo MapReduce", endMapReduceTime);

            // Escribir los resultados
            writeResults(resultList, outPath);

        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            // Cerrar el cliente Hazelcast
            if (client != null) {
                client.shutdown();
            }
        }
    }

    private void writeResults(List<Query1Result> resultList, String outPath) {
        // Escribir los resultados en el archivo de salida
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outPath + "/query1.csv"))) {
            writer.write("Infraction;Agency;Tickets\n");
            for (Query1Result entry : resultList) {
                writer.write(String.format("%s;%s;%d\n",
                        entry.getInfraction(),
                        entry.getAgency(),
                        entry.getTickets()
                ));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
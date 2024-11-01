import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.queries.query1.Query1Collator;
import ar.edu.itba.pod.queries.query1.Query1CombinerFactory;
import ar.edu.itba.pod.queries.query1.Query1Mapper;
import ar.edu.itba.pod.queries.query1.Query1ReducerFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class Query1IntegrationTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void init() {
        // Create a new Hazelcast instance for testing
        Config config = new Config();
        config.setInstanceName("test-instance");
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @AfterClass
    public static void teardown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    public void testMapReduceJob() throws ExecutionException, InterruptedException {
        // Create sample tickets map
        IMap<String, Ticket> ticketsMap = hazelcastInstance.getMap("ticketsTest");

        // Add sample tickets
        Ticket ticket1 = new Ticket("ABC123", "101", "Speeding", 150.0, "TRAFFIC", null, null);
        Ticket ticket2 = new Ticket("DEF456", "102", "Illegal Parking", 100.0, "POLICE", null, null);
        Ticket ticket3 = new Ticket("GHI789", "101", "Speeding", 200.0, "TRAFFIC", null, null);
        Ticket ticket4 = new Ticket("JKL012", "103", "Red Light", 180.0, "POLICE", null, null);
        Ticket ticket5 = new Ticket("MNO345", "101", "Speeding", 150.0, "TRAFFIC", null, null);

        ticketsMap.put("ticket1", ticket1);
        ticketsMap.put("ticket2", ticket2);
        ticketsMap.put("ticket3", ticket3);
        ticketsMap.put("ticket4", ticket4);
        ticketsMap.put("ticket5", ticket5);

        // Execute MapReduce job
        JobTracker jobTracker = hazelcastInstance.getJobTracker("default");
        Job<String, Ticket> job = jobTracker.newJob(KeyValueSource.fromMap(ticketsMap));

        List<Map.Entry<String, Integer>> resultList = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new Query1ReducerFactory())
                .submit(new Query1Collator())
                .get();

        // Expected results:
        // 1. Speeding;TRAFFIC => 3
        // 2. Illegal Parking;POLICE => 1
        // 3. Red Light;POLICE => 1

        assertEquals(3, resultList.size());

        // Verify the results in the expected order
        assertEquals("Speeding;TRAFFIC", resultList.get(0).getKey());
        assertEquals(Integer.valueOf(3), resultList.get(0).getValue());

        assertEquals("Illegal Parking;POLICE", resultList.get(1).getKey());
        assertEquals(Integer.valueOf(1), resultList.get(1).getValue());

        assertEquals("Red Light;POLICE", resultList.get(2).getKey());
        assertEquals(Integer.valueOf(1), resultList.get(2).getValue());
    }
}
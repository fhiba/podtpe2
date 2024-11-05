import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.queries.query3.*;

import ar.edu.itba.pod.models.Ticket;
import static org.junit.Assert.*;
import java.util.Map;


public class Query3IntegrationTest {
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

        LocalDateTime baseDate = LocalDateTime.of(2023, 1, 1, 0, 0);
        
        // Add sample tickets with the correct constructor parameters:
        // (plate, infractionId, description, amount, agency, issueDate, county)
        Ticket ticket1 = new Ticket("plate1", "SPD001", "Speeding", 100.0, "TRAFFIC", baseDate, "County1");
        Ticket ticket2 = new Ticket("plate1", "PRK001", "Parking", 50.0, "POLICE", baseDate, "County1");
        Ticket ticket3 = new Ticket("plate1", "SPD001", "Speeding", 100.0, "TRAFFIC", baseDate, "County1");
        Ticket ticket4 = new Ticket("plate2", "RED001", "Red Light", 150.0, "TRAFFIC", baseDate, "County2");
        Ticket ticket5 = new Ticket("plate3", "SPD001", "Speeding", 100.0, "TRAFFIC", baseDate, "County2");

        ticketsMap.put("ticket1", ticket1);
        ticketsMap.put("ticket2", ticket2);
        ticketsMap.put("ticket3", ticket3);
        ticketsMap.put("ticket4", ticket4);
        ticketsMap.put("ticket5", ticket5);

        // Execute MapReduce job
        JobTracker jobTracker = hazelcastInstance.getJobTracker("default");
        KeyValueSource<String, Ticket> source = KeyValueSource.fromMap(ticketsMap);
        Job<String, Ticket> job = jobTracker.newJob(source);

        // Configure and execute the MapReduce job with proper parameters
        List<String> result = job
                .mapper(new Query3Mapper(baseDate.minusDays(1), baseDate.plusDays(1)))
                .reducer(new Query3ReducerFactory(2))  // Set n=2 for repeat offenders
                .submit(new Query3Collator())
                .get();

        // Verify results
        assertNotNull(result);
        assertTrue(result.size() > 0);
        // County1 should have 100% repeat offenders (plate1 has 3 tickets)
        assertEquals("County1;100.00%", result.get(0));
    }
}

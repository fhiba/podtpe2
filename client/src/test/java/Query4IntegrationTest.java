import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.queries.query4.*;
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

public class Query4IntegrationTest {
    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void init() {
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
        // Create test maps
        IMap<String, Ticket> ticketsMap = hazelcastInstance.getMap("ticketsTest");
        Map<String, Infraction> infractionsMap = new HashMap<>();

        // Add sample infractions
        infractionsMap.put("101", new Infraction("101", "Speeding"));
        infractionsMap.put("102", new Infraction("102", "Parking"));
        infractionsMap.put("103", new Infraction("103", "Red Light"));
        infractionsMap.put("104", new Infraction("104", "Wrong Turn"));
        infractionsMap.put("105", new Infraction("105", "No License"));
        infractionsMap.put("106", new Infraction("106", "Cell Phone"));

        // Add sample tickets for POLICE agency with varying fine amounts
        // Speeding tickets (diff: 150)
        ticketsMap.put("t1", new Ticket("ABC123", "101", "Speeding", 250.0, "POLICE", null, null));
        ticketsMap.put("t2", new Ticket("DEF456", "101", "Speeding", 100.0, "POLICE", null, null));
        
        // Parking tickets (diff: 100)
        ticketsMap.put("t3", new Ticket("GHI789", "102", "Parking", 150.0, "POLICE", null, null));
        ticketsMap.put("t4", new Ticket("JKL012", "102", "Parking", 50.0, "POLICE", null, null));
        
        // Red Light tickets (diff: 100)
        ticketsMap.put("t5", new Ticket("MNO345", "103", "Red Light", 200.0, "POLICE", null, null));
        ticketsMap.put("t6", new Ticket("PQR678", "103", "Red Light", 100.0, "POLICE", null, null));
        
        // Wrong Turn tickets (diff: 80)
        ticketsMap.put("t7", new Ticket("STU901", "104", "Wrong Turn", 180.0, "POLICE", null, null));
        ticketsMap.put("t8", new Ticket("VWX234", "104", "Wrong Turn", 100.0, "POLICE", null, null));
        
        // No License tickets (diff: 50)
        ticketsMap.put("t9", new Ticket("YZA567", "105", "No License", 150.0, "POLICE", null, null));
        ticketsMap.put("t10", new Ticket("BCD890", "105", "No License", 100.0, "POLICE", null, null));
        
        // Cell Phone tickets (should be ignored - different agency)
        ticketsMap.put("t11", new Ticket("EFG123", "106", "Cell Phone", 300.0, "TRAFFIC", null, null));
        ticketsMap.put("t12", new Ticket("HIJ456", "106", "Cell Phone", 100.0, "TRAFFIC", null, null));

        // Execute MapReduce job
        JobTracker jobTracker = hazelcastInstance.getJobTracker("default");
        Job<String, Ticket> job = jobTracker.newJob(KeyValueSource.fromMap(ticketsMap));

        List<String> resultList = job
                .mapper(new Query4Mapper("POLICE"))
                .combiner(new Query4CombinerFactory())
                .reducer(new Query4ReducerFactory())
                .submit(new Query4Collator(5, infractionsMap))
                .get();

        // Expected results (top 5):
        // 1. Speeding - diff 150 (250-100)
        // 2. Red Light - diff 100 (200-100) - comes first alphabetically
        // 3. Parking - diff 100 (150-50)
        // 4. Wrong Turn - diff 80 (180-100)
        // 5. No License - diff 50 (150-100)

        assertEquals(5, resultList.size());
        assertEquals("Speeding;250;100;150", resultList.get(0));
        assertEquals("Red Light;200;100;100", resultList.get(1));
        assertEquals("Parking;150;50;100", resultList.get(2));
        assertEquals("Wrong Turn;180;100;80", resultList.get(3));
        assertEquals("No License;150;100;50", resultList.get(4));
        
        // Additional verifications
        assertFalse("Results should not contain Cell Phone tickets (different agency)", 
            resultList.stream().anyMatch(s -> s.contains("Cell Phone")));
    }
}

import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.queries.query2.Query2Collator;
import ar.edu.itba.pod.queries.query2.Query2CombinerFactory;
import ar.edu.itba.pod.queries.query2.Query2Mapper;
import ar.edu.itba.pod.queries.query2.Query2ReducerFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class Query2IntegrationTest {

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

        // Create sample dates
        LocalDateTime date1 = LocalDateTime.of(2023, 1, 15, 10, 0);
        LocalDateTime date2 = LocalDateTime.of(2023, 2, 20, 14, 0);
        LocalDateTime date3 = LocalDateTime.of(2023, 2, 25, 16, 0);
        LocalDateTime date4 = LocalDateTime.of(2023, 3, 1, 9, 0);
        LocalDateTime date5 = LocalDateTime.of(2023, 3, 15, 11, 0);
        LocalDateTime date6 = LocalDateTime.of(2022, 12, 31, 23, 59);

        // Add sample tickets with more varied scenarios
        Ticket ticket1 = new Ticket("ABC123", "101", "Speeding", 100.0, "TRAFFIC", date1, "County1");
        Ticket ticket2 = new Ticket("DEF456", "102", "Parking", 150.0, "POLICE", date1, "County2");
        Ticket ticket3 = new Ticket("GHI789", "103", "Speeding", 200.0, "TRAFFIC", date2, "County1");
        Ticket ticket4 = new Ticket("JKL012", "104", "Red Light", 180.0, "POLICE", date2, "County3");
        Ticket ticket5 = new Ticket("MNO345", "105", "Speeding", 150.0, "TRAFFIC", date3, "County1");
        // Adding tickets for March to test YTD accumulation
        Ticket ticket6 = new Ticket("PQR678", "106", "Parking", 120.0, "POLICE", date4, "County2");
        Ticket ticket7 = new Ticket("STU901", "107", "Speeding", 90.0, "TRAFFIC", date5, "County1");
        // Adding a ticket from previous year
        Ticket ticket8 = new Ticket("VWX234", "108", "Parking", 200.0, "POLICE", date6, "County2");

        ticketsMap.put("ticket1", ticket1);
        ticketsMap.put("ticket2", ticket2);
        ticketsMap.put("ticket3", ticket3);
        ticketsMap.put("ticket4", ticket4);
        ticketsMap.put("ticket5", ticket5);
        ticketsMap.put("ticket6", ticket6);
        ticketsMap.put("ticket7", ticket7);
        ticketsMap.put("ticket8", ticket8);

        // Execute MapReduce job
        JobTracker jobTracker = hazelcastInstance.getJobTracker("default");
        KeyValueSource<String, Ticket> source = KeyValueSource.fromMap(ticketsMap);
        Job<String, Ticket> job = jobTracker.newJob(source);

        // Configure and execute the MapReduce job
        List<String> result = job
                .mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory())
                .reducer(new Query2ReducerFactory())
                .submit(new Query2Collator())
                .get();

        // Verify results
        assertNotNull(result);
        
        // Expected format: "AGENCY;YEAR;MONTH;YTD_AMOUNT"
        List<String> expected = Arrays.asList(
            "POLICE;2022;12;200",
            "POLICE;2023;1;150",
            "POLICE;2023;2;330",
            "POLICE;2023;3;450",
            "TRAFFIC;2023;1;100",
            "TRAFFIC;2023;2;450",
            "TRAFFIC;2023;3;540"
        );
        
        assertEquals(expected.size(), result.size());
        assertEquals(expected, result);
    }
}

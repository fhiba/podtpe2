package ar.edu.itba.pod.client.utils;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimestampWriter {

    /**
     * Writes a timestamp with a message to the specified file.
     *
     * @param filePath  the path to the file where the timestamp will be written
     * @param message   the message to associate with the timestamp
     * @param timestamp the timestamp to write
     */
    public static void writeTimestamp(String filePath, String message, LocalDateTime timestamp) {
        // Define the formatter for the timestamp
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss:SSSS");
        // Format the timestamp
        String formattedTimestamp = timestamp.format(formatter);
        // Create the log message
        String logMessage = String.format("%s INFO - %s%n", formattedTimestamp, message);
        // Write the log message to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            writer.write(logMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
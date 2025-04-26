package broker;

import message.Message;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileMessageBroker {
    private final String[] consumerFiles = new String[10]; // Max 10 consumers for example
    private int consumerCount = 0;

    public synchronized void registerConsumer(String consumerId) {
        if (consumerCount >= consumerFiles.length) {
            throw new RuntimeException("Maximum number of consumers reached.");
        }

        String path = "data/" + consumerId + ".txt";
        consumerFiles[consumerCount++] = path;
    }

    public synchronized void publish(Message message) {
        if (message == null) throw new IllegalArgumentException("Null message");

        for (int i = 0; i < consumerCount; i++) {
            String filename = consumerFiles[i];
            File file = new File(filename);

            try (FileWriter writer = new FileWriter(file, true)) {
                writer.write(message.getContent() + System.lineSeparator());
            } catch (IOException e) {
                System.err.println("Error writing to " + filename + ": " + e.getMessage());
            }
        }
    }
}

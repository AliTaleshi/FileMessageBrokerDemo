package broker;

import message.Message;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileMessageBroker {
    private final List<String> consumerFiles = new ArrayList<>();

    public synchronized void registerConsumer(String consumerId) {
        consumerFiles.add("data/" + consumerId + ".queue");
    }

    public synchronized void publish(Message message) {
        if (message == null) throw new IllegalArgumentException("Null message");

        for (String filename : consumerFiles) {
            try (FileWriter writer = new FileWriter(filename, true)) {
                writer.write(message.getContent() + System.lineSeparator());
            } catch (IOException e) {
                System.err.println("Error writing to " + filename + ": " + e.getMessage());
            }
        }
    }
}

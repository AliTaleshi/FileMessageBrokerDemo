package broker;

import message.Message;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileMessageBroker {
    private final List<String> consumerFiles = new ArrayList<>();

    public synchronized void registerConsumer(String consumerId) {
        String path = "data/" + consumerId + ".txt";
        consumerFiles.add(path);
    }

    public synchronized void publish(Message message) {
        if (message == null) throw new IllegalArgumentException("Null message");

        for (String filename : consumerFiles) {
            File file = new File(filename);
            try(FileWriter writer = new FileWriter(file, true)) {
                writer.write(message.getContent() + System.lineSeparator());
            } catch (IOException e) {
                System.err.println("Error writing to " + filename + ": " + e.getMessage());
            }
        }
    }
}

package broker;

import message.Message;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileMessageBroker {
    private final String[] consumerFiles = new String[10];
    private int consumerFilesIndex = 0;

    public synchronized void registerConsumer(String consumerId) {
        String path = "data/" + consumerId + ".txt";

        if (consumerFilesIndex == consumerFiles.length - 1) {
            throw new ArrayIndexOutOfBoundsException("No more consumer files allowed to create!");
        }

        consumerFiles[consumerFilesIndex++] = path;
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

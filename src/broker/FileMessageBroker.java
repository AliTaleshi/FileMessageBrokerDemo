package broker;

import message.Message;

import java.io.*;

public class FileMessageBroker {
    private final String[] consumerFiles = new String[10];
    private final int[] consumerMessageCounts = new int[10];
    private final int maxMessagesPerFile;
    private int consumerCount = 0;

    public FileMessageBroker(int maxMessagesPerFile) {
        this.maxMessagesPerFile = maxMessagesPerFile;
    }

    public synchronized void registerConsumer(String consumerId) {
        if (consumerCount >= consumerFiles.length) {
            throw new RuntimeException("Maximum consumers exceeded.");
        }

        String path = "data/" + consumerId + ".txt";
        File file = new File(path);

        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        try {
            if (!file.exists()) file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        consumerFiles[consumerCount] = path;
        consumerMessageCounts[consumerCount] = countLines(file);
        consumerCount++;
    }

    public synchronized void publish(Message message) throws InterruptedException {
        if (message == null) throw new IllegalArgumentException("Null message");

        for (int i = 0; i < consumerCount; i++) {
            while (consumerMessageCounts[i] >= maxMessagesPerFile) {
                System.out.println("Producer waiting: File full for " + consumerFiles[i]);
                wait();
            }

            try (FileWriter writer = new FileWriter(consumerFiles[i], true)) {
                writer.write(message.getContent() + System.lineSeparator());
                consumerMessageCounts[i]++;
            } catch (IOException e) {
                System.err.println("Error writing to " + consumerFiles[i] + ": " + e.getMessage());
            }
        }

        notifyAll();
    }

    public synchronized String consume(String consumerId) throws InterruptedException {
        int index = getConsumerIndex(consumerId);
        if (index == -1) throw new IllegalArgumentException("Unknown consumer: " + consumerId);

        String path = consumerFiles[index];
        File file = new File(path);

        while (consumerMessageCounts[index] == 0 || file.length() == 0) {
            System.out.println("Consumer waiting: File empty for " + consumerId);
            wait();
        }

        String consumedMessage = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            StringBuilder remaining = new StringBuilder();
            boolean firstLine = true;

            while ((line = reader.readLine()) != null) {
                if (firstLine) {
                    consumedMessage = line;
                    firstLine = false;
                } else {
                    remaining.append(line).append(System.lineSeparator());
                }
            }
            reader.close();

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, false));
            writer.write(remaining.toString());
            writer.close();

            consumerMessageCounts[index]--;
        } catch (IOException e) {
            e.printStackTrace();
        }

        notifyAll();
        return consumedMessage;
    }

    private int countLines(File file) {
        int lines = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while (reader.readLine() != null) lines++;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    private int getConsumerIndex(String consumerId) {
        for (int i = 0; i < consumerCount; i++) {
            if (consumerFiles[i].contains(consumerId)) {
                return i;
            }
        }
        return -1;
    }
}

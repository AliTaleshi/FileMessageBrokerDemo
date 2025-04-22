package consumer;

import java.io.*;

public class Consumer implements Runnable {
    private final File queueFile;

    public Consumer(String consumerId, broker.FileMessageBroker broker) {
        this.queueFile = new File("data/" + consumerId + ".txt");
        initQueue(queueFile);
        broker.registerConsumer(consumerId);
    }

    private void initQueue(File queueFile) {
        File parentDir = queueFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        if (!queueFile.exists()) {
            try {
                queueFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        try (RandomAccessFile reader = new RandomAccessFile(queueFile, "rw")) {
            long lastReadPos = 0;

            while (true) {
                Thread.sleep(5000L);
                long fileLength = queueFile.length();

                if (fileLength > lastReadPos) {
                    reader.seek(lastReadPos);
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(Thread.currentThread().getName() + " received: " + line);
                    }

                    // After consuming, truncate the file
                    reader.setLength(0); // Clears the file
                    lastReadPos = 0;
                }

                Thread.sleep(500); // Avoid busy spinning
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
        }
    }
}

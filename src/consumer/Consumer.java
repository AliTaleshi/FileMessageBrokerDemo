package consumer;

import java.io.File;
import java.io.RandomAccessFile;

public class Consumer implements Runnable {
    private final File queueFile;

    public Consumer(String consumerId, broker.FileMessageBroker broker) {
        this.queueFile = new File("data/" + consumerId + ".queue");
        broker.registerConsumer(consumerId);
    }

    @Override
    public void run() {
        try (RandomAccessFile reader = new RandomAccessFile(queueFile, "r")) {
            long lastReadPos = 0;

            while (true) {
                long fileLength = queueFile.length();

                if (fileLength > lastReadPos) {
                    reader.seek(lastReadPos);
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(Thread.currentThread().getName() + " received: " + line);
                    }
                    lastReadPos = reader.getFilePointer();
                }

                Thread.sleep(500); // Small delay to reduce busy-waiting
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
        }
    }
}

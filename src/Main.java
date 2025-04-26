import broker.FileMessageBroker;
import consumer.Consumer;
import producer.Producer;

public class Main {
    public static void main(String[] args) {
        FileMessageBroker broker = new FileMessageBroker();

        // Start Consumers
        for (int i = 0; i < 3; i++) {
            String consumerId = "Consumer-" + i;
            Thread consumerThread = new Thread(new Consumer(consumerId, broker), consumerId);
            consumerThread.start();
        }

        // Start Producers
        for (int i = 0; i < 2; i++) {
            String producerName = "Producer-" + i;
            Thread producerThread = new Thread(new Producer(broker, producerName));
            producerThread.start();
        }
    }
}

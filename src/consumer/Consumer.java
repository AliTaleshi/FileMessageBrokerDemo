package consumer;

import broker.FileMessageBroker;

import java.util.TreeMap;

public class Consumer implements Runnable {
    private final String consumerId;
    private final FileMessageBroker broker;

    public Consumer(String consumerId, FileMessageBroker broker) {
        this.consumerId = consumerId;
        this.broker = broker;
        broker.registerConsumer(consumerId);
    }

    @Override
    public void run() {
        try {
            while (true) {
                String message = broker.consume(consumerId);
                if (message != null) {
                    System.out.println(Thread.currentThread().getName() + " consumed: " + message);
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

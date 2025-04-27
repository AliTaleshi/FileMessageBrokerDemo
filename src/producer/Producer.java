package producer;

import message.TextMessage;
import broker.FileMessageBroker;

public class Producer implements Runnable {
    private final FileMessageBroker broker;
    private final String name;

    public Producer(FileMessageBroker broker, String name) {
        this.broker = broker;
        this.name = name;
    }

    @Override
    public void run() {
        int count = 0;

        try {
            while (true) {
                String content = name + " message #" + count++;
                broker.publish(new TextMessage(content));
                System.out.println("Producer: " + name + " published: " + content);
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

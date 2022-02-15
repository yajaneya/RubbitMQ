import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Subscriber {
    private final static String QUEUE_NAME = "topic";
    private static final String EXCHANGE_NAME = "topic_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String queueName = channel.queueDeclare().getQueue();
        System.out.println("QUEUE NAME: " + queueName);

        String command = "";
        Scanner scanner = new Scanner(System.in);
        while (!(command = scanner.nextLine()).equals("quit")) {
            String[] comm = command.split(" ");
            if (comm[0].equals("set_topic")) {
                String routingKey = comm[1];
                channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                System.out.println(" [*] Waiting for messages with routing key (" + routingKey + "):");
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                };
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                });
            }
        }
    }

}

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

        String command;
        Scanner scanner = new Scanner(System.in);
        List<String> subs = new ArrayList<>();
        while (!(command = scanner.nextLine()).equals("quit")) {
            String[] comm = command.split(" ");
            if (comm[0].equals("set_topic")) {
                String routingKey = comm[1];
                subs.add(routingKey);
                channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                System.out.println(" [*] Waiting for messages with routing key (" + routingKey + "):");
                delivery(channel, queueName);
            }
            if (comm[0].equals("unset_topic")) {
                String routingKey = comm[1];
                if (unsuscribe(channel, queueName, subs, routingKey)) {
                    suscribe(channel, subs);
                }
            }
        }
    }

    private static void suscribe(Channel channel, List<String> subs) throws IOException {
        String queueName = channel.queueDeclare().getQueue();
        subs.forEach(s-> {
            try {
                channel.queueBind(queueName, EXCHANGE_NAME, s);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        delivery(channel, queueName);
    }

    private static void delivery(Channel channel, String queueName) throws IOException {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    private static boolean unsuscribe(Channel channel, String queueName, List<String> subs, String routingKey) {
        for (int i=0; i<subs.size(); i++) {
            if (subs.get(i).equals(routingKey)) {
                subs.remove(i);
                try {
                    channel.queueDelete(queueName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return true;
            }
        }
        return false;
    }

}

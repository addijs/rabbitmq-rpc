package br.edu.ifpb.adilson;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RPCServer {

    private static String QUEUE_NAME = "rpc_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("mqadmin");
        factory.setPassword("586467");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queuePurge(QUEUE_NAME);

        channel.basicQos(1);

        System.out.println("Esperando requisições RPC...\n");
        Object monitor = new Object();

        DeliverCallback callback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            try {
                String deliveryMessage = new String(delivery.getBody(), "UTF-8");

                channel.basicPublish("",
                        delivery.getProperties().getReplyTo(),
                        replyProps,
                        String.format("Bem-vindo, %s", deliveryMessage).getBytes());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                System.out.println("Resolvido requisição ID " + delivery.getProperties().getCorrelationId());
            } catch (Exception err) {
                System.out.println(err);
            }
        };

        channel.basicConsume(QUEUE_NAME, false, callback, consumerTag -> {});

        while (true) {
            synchronized (monitor) {
                try {
                    monitor.wait();
                } catch (InterruptedException err) {
                    err.printStackTrace();
                }
            }
        }
    }
}

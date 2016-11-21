package com.test;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

@Profile("with-rpc")
@Service
@Slf4j
public class CalculatorProcessorService {

    @Value("${rabbitmq.host}")
    private String host;

    @Value("${rabbitmq.queue.rpc.name}")
    private String queueName;

    private Connection connection;
    private Channel channel;

    @PostConstruct
    public void init() throws Exception {
        log.info("####-CalculatorProcessorService started");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);

        channel.basicQos(1);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(properties.getCorrelationId())
                        .build();

                String message = new String(body, "UTF-8");
                log.info(String.format("compute fibonacci(%s)...", message));

                int n = Integer.parseInt(message);

                String response = "" + fib(n);
                log.info(String.format("fibonacci(%s) = %s", message, response));

                channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        channel.basicConsume(queueName, false, consumer);
    }

    @PreDestroy
    public void tearDown() throws Exception {
        log.info("####-CalculatorProcessorService stopped");

        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }


}

package com.test;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

@Profile("with-log")
@Service
@Slf4j
public class LogProcessorService{

    @Value("${rabbitmq.host}")
    private String host;

    @Value("${rabbitmq.exchange.log.name}")
    private String exchangeName;

    @Value("${rabbitmq.exchange.log.type}")
    private String exchangeType;

    private Connection connection;
    private Channel channel;

    @PostConstruct
    public void init() throws Exception {
        log.info("####-LogProcessorService started");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        connection = factory.newConnection();
        channel = connection.createChannel();

        consume();
    }

    @PreDestroy
    public void tearDown() throws Exception {
        log.info("####-LogProcessorService stopped");
        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public void consume() throws Exception {

        channel.exchangeDeclare(exchangeName, exchangeType);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, exchangeName, "");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                log.info(" [x] Received '" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }



}

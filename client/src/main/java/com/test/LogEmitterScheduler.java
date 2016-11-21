package com.test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Profile("with-log")
@Service
@Slf4j
public class LogEmitterScheduler {

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
        log.info("####-LogEmitterScheduler started");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    @PreDestroy
    public void tearDown() throws Exception {
        log.info("####-LogEmitterScheduler stopped");

        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Scheduled(cron = "0/10 * * * * *")
    public void send() throws Exception {

        channel.exchangeDeclare(exchangeName, exchangeType);

        String message = "Hello World!";

        channel.basicPublish(exchangeName, "", null, message.getBytes());
        log.info(" [x] Sent '" + message + "'");
    }
}

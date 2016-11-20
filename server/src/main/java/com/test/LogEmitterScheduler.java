package com.test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
@Slf4j
public class LogEmitterScheduler {


    @Value("${rabbitmq.host}")
    private String host;

    @Value("${rabbitmq.exchange.name}")
    private String exchangeName;

    @Value("${rabbitmq.exchange.type}")
    private String exchangeType;

    @PostConstruct
    public void init() throws Exception {
        log.info("LogEmitterScheduler started");
    }

    @Scheduled(cron = "0/10 * * * * *")
    public void send() throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName, exchangeType);

        String message = "Hello World!";

        channel.basicPublish(exchangeName, "", null, message.getBytes());
        log.info(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}

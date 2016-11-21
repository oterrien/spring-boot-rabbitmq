package com.test;

import com.rabbitmq.client.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@Profile("with-rpc")
@Service
@Slf4j
public class CalculatorLauncherService {

    @Value("${rabbitmq.host}")
    private String host;

    @Value("${rabbitmq.queue.rpc.name}")
    private String queueName;

    private Connection connection;
    private Channel channel;

    private String replyQueueName;

    private Call call = new Call();

    @Data
    class Call {
        private String correlationId;
        private String message;
        private String result;
    }

    private CountDownLatch doneSignal;

    @PostConstruct
    public void init() throws Exception {
        log.info("####-CalculatorLauncherService started");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        connection = factory.newConnection();
        channel = connection.createChannel();

        replyQueueName = channel.queueDeclare().getQueue();

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                String response = new String(body, "UTF-8");

                Optional.of(call).
                        filter(p -> p.getCorrelationId().equalsIgnoreCase(properties.getCorrelationId())).
                        ifPresent(p -> {
                            p.setResult(response);
                            doneSignal.countDown();
                        });
            }
        };
        channel.basicConsume(replyQueueName, true, consumer);
    }

    @PreDestroy
    public void tearDown() throws Exception {
        log.info("####-CalculatorLauncherService stopped");

        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Scheduled(cron = "0/1 * * * * *")
    public void call() throws Exception {

        doneSignal = new CountDownLatch(1);

        final String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        String message = "46";
        log.info(String.format("call fibonacci(%s)...", message));

        call.setCorrelationId(props.getCorrelationId());

        channel.basicPublish("", queueName, props, message.getBytes("UTF-8"));

        doneSignal.await();

        String result = call.getResult();

        log.info(String.format("fibonacci(%s) = %s", message, result));

    }


}

package com.apple.springboot.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@Service
public class SqsService {

    private static final Logger logger = LoggerFactory.getLogger(SqsService.class);

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final String queueUrl;

    public SqsService(SqsClient sqsClient,
                      ObjectMapper objectMapper,
                      @Value("${aws.sqs.queue.url}") String queueUrl) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.queueUrl = queueUrl;
    }

    public void sendMessage(Object messagePayload) {
        try {
            String messageBody = objectMapper.writeValueAsString(messagePayload);
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build();
            sqsClient.sendMessage(sendMessageRequest);
            logger.info("Successfully sent message to SQS queue.");
        } catch (JsonProcessingException e) {
            logger.error("Error serializing message payload to JSON", e);
        } catch (Exception e) {
            logger.error("Error sending message to SQS queue", e);
        }
    }
}
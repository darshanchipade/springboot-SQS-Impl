package com.apple.springboot.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

@Service
public class SqsService {

    private static final Logger logger = LoggerFactory.getLogger(SqsService.class);

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final String queueUrl;

    /**
     * Creates an SQS publisher bound to the configured queue URL.
     */
    public SqsService(SqsClient sqsClient,
                      ObjectMapper objectMapper,
                      @Value("${aws.sqs.queue.url}") String queueUrl) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.queueUrl = queueUrl;
    }

    /**
     * Sends a single message payload to the SQS queue.
     */
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

    /**
     * Sends a list of payloads in batches of up to 10 messages.
     */
    public <T> void sendMessages(java.util.List<T> messagePayloads) {
        if (messagePayloads == null || messagePayloads.isEmpty()) {
            return;
        }
        java.util.List<java.util.List<T>> batches = partition(messagePayloads, 10);
        for (java.util.List<T> batch : batches) {
            try {
                java.util.List<SendMessageBatchRequestEntry> entries = new java.util.ArrayList<>();
                for (int i = 0; i < batch.size(); i++) {
                    T payload = batch.get(i);
                    String body = objectMapper.writeValueAsString(payload);
                    entries.add(SendMessageBatchRequestEntry.builder()
                            .id("msg-" + i + "-" + java.util.UUID.randomUUID())
                            .messageBody(body)
                            .build());
                }
                SendMessageBatchRequest request = SendMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(entries)
                        .build();
                sqsClient.sendMessageBatch(request);
                logger.info("Successfully sent batch of {} messages to SQS queue.", entries.size());
            } catch (JsonProcessingException e) {
                logger.error("Error serializing batch message payload to JSON", e);
            } catch (Exception e) {
                logger.error("Error sending batch message to SQS queue", e);
            }
        }
    }

    /**
     * Splits a list into fixed-size sublists for batch requests.
     */
    private <T> java.util.List<java.util.List<T>> partition(java.util.List<T> list, int size) {
        java.util.List<java.util.List<T>> parts = new java.util.ArrayList<>();
        if (list == null || list.isEmpty() || size <= 0) {
            return parts;
        }
        for (int i = 0; i < list.size(); i += size) {
            parts.add(list.subList(i, Math.min(list.size(), i + size)));
        }
        return parts;
    }
}
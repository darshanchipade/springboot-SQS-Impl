package com.apple.springboot.service;

import com.apple.springboot.model.EnrichmentMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.core.task.TaskRejectedException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
import java.util.Map;

@Service
public class SQSEnrichmentListener {

    private static final Logger logger = LoggerFactory.getLogger(SQSEnrichmentListener.class);

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final EnrichmentProcessor enrichmentProcessor;
    private final TaskExecutor taskExecutor;

    @Value("${aws.sqs.queue.url}")
    private String queueUrl;

    @Value("${aws.sqs.listener.batch-size:5}")
    private int batchSize;

    public SQSEnrichmentListener(SqsClient sqsClient,
                                 ObjectMapper objectMapper,
                                 EnrichmentProcessor enrichmentProcessor,
                                 @Qualifier("sqsMessageProcessorExecutor") TaskExecutor taskExecutor) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.enrichmentProcessor = enrichmentProcessor;
        this.taskExecutor = taskExecutor;
    }

    @Scheduled(fixedDelay = 2000)
    public void pollQueue() {
        try {
            if (isWorkerSaturated()) {
                logger.debug("Worker pool saturated; skipping this poll.");
                return;
            }

            int batch = Math.max(1, Math.min(batchSize, 10));
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(batch)
                    .waitTimeSeconds(20)
                    .visibilityTimeout(180)
                    .attributeNamesWithStrings("ApproximateReceiveCount")
                    .build();

            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            logger.debug("Polled SQS and received {} messages (batch size {}).", messages.size(), batch);

            for (Message message : messages) {
                try {
                    taskExecutor.execute(() -> processMessage(message));
                } catch (TaskRejectedException tre) {
                    // Fallback: process inline to avoid losing the task
                    logger.warn("Executor saturated; processing message {} inline.", message.messageId());
                    processMessage(message);
                }
            }
        } catch (Exception e) {
            logger.error("Error polling SQS queue", e);
        }
    }

    private boolean isWorkerSaturated() {
        if (taskExecutor instanceof ThreadPoolTaskExecutor ex) {
            int active = ex.getActiveCount();
            int maxPool = ex.getMaxPoolSize();
            int remainingCapacity = ex.getThreadPoolExecutor().getQueue().remainingCapacity();
            boolean poolFull = active >= maxPool;
            boolean queueFull = remainingCapacity <= 0;
            return poolFull && queueFull;
        }
        return false;
    }

    private void processMessage(Message message) {
        try {
            EnrichmentMessage enrichmentMessage = objectMapper.readValue(message.body(), EnrichmentMessage.class);
            enrichmentProcessor.process(enrichmentMessage);
            deleteMessage(message);
        } catch (ThrottledException te) {
            int delaySec = computeVisibilityExtensionSeconds(message);
            try {
                sqsClient.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .visibilityTimeout(delaySec)
                        .build());
                logger.warn("Throttled; extended visibility {}s for message {}.", delaySec, message.messageId());
            } catch (Exception visErr) {
                logger.error("Failed to change visibility for message {}: {}", message.messageId(), visErr.getMessage(), visErr);
            }
        } catch (Exception e) {
            logger.error("Error processing message {}: {}", message.messageId(), e.getMessage(), e);
        }
    }

    private int computeVisibilityExtensionSeconds(Message message) {
        try {
            Map<String, String> attrs = message.attributesAsStrings();
            String receiveCountStr = attrs != null ? attrs.get("ApproximateReceiveCount") : null;
            int receiveCount = receiveCountStr != null ? Math.max(1, Integer.parseInt(receiveCountStr)) : 1;
            int[] schedule = new int[]{30, 60, 120, 240, 300};
            int idx = Math.min(receiveCount - 1, schedule.length - 1);
            return schedule[idx];
        } catch (Exception e) {
            return 180;
        }
    }

    private void deleteMessage(Message message) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
            logger.debug("Successfully deleted message {} from the queue.", message.messageId());
        } catch (Exception e) {
            logger.error("Failed to delete message {} from SQS queue: {}", message.messageId(), e.getMessage(), e);
        }
    }
}
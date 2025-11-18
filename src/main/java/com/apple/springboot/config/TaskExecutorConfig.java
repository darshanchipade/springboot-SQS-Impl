package com.apple.springboot.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class TaskExecutorConfig {

    @Bean("sqsMessageProcessorExecutor")
    public TaskExecutor sqsMessageProcessorExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5); // Start with 5 threads
        executor.setMaxPoolSize(8); // Allow up to 10 threads
        executor.setQueueCapacity(500); // Queue up to 25 tasks before blocking
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("SQSWorker-");
        executor.initialize();
        return executor;
    }
}
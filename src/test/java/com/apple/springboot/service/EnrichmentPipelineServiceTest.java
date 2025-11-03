package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ContentHash;
import com.apple.springboot.model.EnrichmentMessage;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentHashRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EnrichmentPipelineServiceTest {

    @Mock
    private CleansedDataStoreRepository cleansedDataStoreRepository;

    @Mock
    private EnrichedContentElementRepository enrichedContentElementRepository;

    @Mock
    private SqsService sqsService;

    @Mock
    private EnrichmentCompletionService completionService;

    @Mock
    private ContentHashRepository contentHashRepository;

    private ObjectMapper objectMapper;

    private EnrichmentPipelineService enrichmentPipelineService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        when(cleansedDataStoreRepository.save(any(CleansedDataStore.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        enrichmentPipelineService = new EnrichmentPipelineService(
                cleansedDataStoreRepository,
                enrichedContentElementRepository,
                objectMapper,
                sqsService,
                completionService,
                contentHashRepository,
                false,
                false
        );
    }

    @Test
    void skipsItemsWhenHashesAlreadyMatch() throws Exception {
        CleansedDataStore cleansed = buildCleansedStore();
        String sourcePath = "/content/foo";
        String fieldName = "copy";
        String usagePath = "/section";
        String contentHashValue = "abc";
        String contextHashValue = "ctx";

        cleansed.setCleansedItems(List.of(buildItem(sourcePath, fieldName, usagePath, contentHashValue, contextHashValue)));

        when(enrichedContentElementRepository.existsByItemSourcePathAndItemOriginalFieldNameAndCleansedText(sourcePath, fieldName, "Hello World"))
                .thenReturn(false);

        ContentHash storedHash = new ContentHash();
        storedHash.setSourcePath(sourcePath);
        storedHash.setItemType(fieldName);
        storedHash.setUsagePath(usagePath);
        storedHash.setContentHash(contentHashValue);
        storedHash.setContextHash(contextHashValue);

        when(contentHashRepository.findBySourcePathAndItemTypeAndUsagePath(sourcePath, fieldName, usagePath))
                .thenReturn(Optional.of(storedHash));

        enrichmentPipelineService.enrichAndStore(cleansed);

        verify(completionService).startTracking(eq(cleansed.getId()), eq(0));
        verify(sqsService, never()).sendMessage(any());
    }

    @Test
    void queuesItemsWhenContentHashDiffers() throws Exception {
        CleansedDataStore cleansed = buildCleansedStore();
        String sourcePath = "/content/foo";
        String fieldName = "copy";
        String usagePath = "/section";

        cleansed.setCleansedItems(List.of(buildItem(sourcePath, fieldName, usagePath, "abc", "ctx")));

        when(enrichedContentElementRepository.existsByItemSourcePathAndItemOriginalFieldNameAndCleansedText(sourcePath, fieldName, "Hello World"))
                .thenReturn(false);

        ContentHash storedHash = new ContentHash();
        storedHash.setSourcePath(sourcePath);
        storedHash.setItemType(fieldName);
        storedHash.setUsagePath(usagePath);
        storedHash.setContentHash("different-hash");
        storedHash.setContextHash("ctx");

        when(contentHashRepository.findBySourcePathAndItemTypeAndUsagePath(sourcePath, fieldName, usagePath))
                .thenReturn(Optional.of(storedHash));

        enrichmentPipelineService.enrichAndStore(cleansed);

        verify(completionService).startTracking(eq(cleansed.getId()), eq(1));

        ArgumentCaptor<EnrichmentMessage> captor = ArgumentCaptor.forClass(EnrichmentMessage.class);
        verify(sqsService).sendMessage(captor.capture());

        EnrichmentMessage message = captor.getValue();
        assertThat(message).isNotNull();
        assertThat(message.getCleansedItemDetail()).isNotNull();
        assertThat(message.getCleansedItemDetail().sourcePath).isEqualTo(sourcePath);
    }

    private CleansedDataStore buildCleansedStore() {
        CleansedDataStore cleansed = new CleansedDataStore();
        cleansed.setId(UUID.randomUUID());
        cleansed.setStatus("CLEANSED_PENDING_ENRICHMENT");
        return cleansed;
    }

    private Map<String, Object> buildItem(String sourcePath,
                                          String fieldName,
                                          String usagePath,
                                          String contentHash,
                                          String contextHash) {
        Map<String, Object> item = new HashMap<>();
        item.put("sourcePath", sourcePath);
        item.put("originalFieldName", fieldName);
        item.put("cleansedContent", "Hello World");
        item.put("model", "test-model");
        item.put("usagePath", usagePath);
        item.put("contentHash", contentHash);
        item.put("contextHash", contextHash);

        Map<String, Object> envelope = new HashMap<>();
        envelope.put("usagePath", usagePath);

        Map<String, Object> context = new HashMap<>();
        context.put("envelope", envelope);
        context.put("facets", Map.of());

        item.put("context", context);
        return item;
    }
}

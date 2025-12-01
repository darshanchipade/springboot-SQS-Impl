package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.QueryInterpretation;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChatbotServiceTest {

    @Mock
    private VectorSearchService vectorSearchService;

    @Mock
    private ConsolidatedEnrichedSectionRepository consolidatedRepo;

    @Mock
    private QueryInterpretationService queryInterpretationService;

    private ChatbotService chatbotService;

    @BeforeEach
    void setUp() {
        chatbotService = new ChatbotService(vectorSearchService, consolidatedRepo, queryInterpretationService);
    }

    @Test
    void queryReturnsBothVersionsWhenCleansedTextDiffers() {
        ChatbotRequest request = new ChatbotRequest();
        request.setMessage("Give me accordion section headline for iPad for Korea");

        Map<String, Object> interpretationContext = Map.of(
                "facets", Map.of("sectionKey", "accordion-section")
        );
        QueryInterpretation interpretation = new QueryInterpretation(
                request.getMessage(),
                "accordion-section",
                null,
                null,
                "headline",
                null,
                null,
                "KR",
                List.of(),
                List.of(),
                interpretationContext
        );
        when(queryInterpretationService.interpret(anyString(), anyMap()))
                .thenReturn(Optional.of(interpretation));

        ConsolidatedEnrichedSection latest = buildSection("iPad Prod Test");
        ConsolidatedEnrichedSection previous = buildSection("iPad Pro");

        ContentChunk latestChunk = ContentChunk.builder()
                .consolidatedEnrichedSection(latest)
                .chunkText(latest.getCleansedText())
                .build();

        when(vectorSearchService.search(
                anyString(),
                isNull(),
                anyInt(),
                isNull(),
                isNull(),
                isNull(),
                isNull(),
                anyString()
        )).thenReturn(List.of(new ContentChunkWithDistance(latestChunk, 0.1)));

        when(consolidatedRepo.findBySectionKey(anyString(), anyInt())).thenReturn(List.of(previous));
        when(consolidatedRepo.findByContextSectionKey(anyString(), anyInt())).thenReturn(List.of());

        List<ChatbotResultDto> results = chatbotService.query(request);

        assertThat(results)
                .extracting(ChatbotResultDto::getCleansedText)
                .containsExactlyInAnyOrder("iPad Prod Test", "iPad Pro");
    }

    private ConsolidatedEnrichedSection buildSection(String text) {
        Map<String, Object> facets = new HashMap<>();
        facets.put("country", "KR");
        facets.put("locale", "ko_KR");
        facets.put("sectionKey", "accordion-section");

        Map<String, Object> context = new HashMap<>();
        context.put("facets", facets);

        ConsolidatedEnrichedSection section = new ConsolidatedEnrichedSection();
        section.setId(UUID.randomUUID());
        section.setCleansedDataId(UUID.randomUUID());
        section.setSectionPath("/content/dam/applecom-cms/live/ko_KR/ipad/accordion");
        section.setSectionUri(section.getSectionPath());
        section.setOriginalFieldName("headline");
        section.setCleansedText(text);
        section.setContext(context);
        section.setSavedAt(OffsetDateTime.now());
        return section;
    }
}

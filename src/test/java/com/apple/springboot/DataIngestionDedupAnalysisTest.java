package com.apple.springboot;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.service.DataIngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class DataIngestionDedupAnalysisTest {

    @Autowired
    private DataIngestionService dataIngestionService;

    @Test
    void reportsDuplicateKeysForInternalJson() throws Exception {
        CleansedDataStore store = dataIngestionService.ingestAndCleanseSingleFile();
        assertNotNull(store);
        assertNotNull(store.getCleansedItems());

        List<Map<String, Object>> items = store.getCleansedItems();
        int total = items.size();

        Map<String, Integer> counts = new HashMap<>();
        for (Map<String, Object> item : items) {
            if (item == null) continue;
            Object sp = item.get("sourcePath");
            Object fn = item.get("originalFieldName");
            if (!(sp instanceof String) || !(fn instanceof String)) continue;
            String key = sp + "::" + fn;
            counts.merge(key, 1, Integer::sum);
        }

        long unique = counts.size();
        long dupKeys = counts.values().stream().filter(v -> v > 1).count();
        long dupItems = counts.values().stream().filter(v -> v > 1).mapToLong(v -> (long) v - 1L).sum();

        System.out.println("Extracted items total: " + total);
        System.out.println("Unique (sourcePath::originalFieldName): " + unique);
        System.out.println("Duplicate keys: " + dupKeys);
        System.out.println("Duplicate items beyond first: " + dupItems);

        // Sanity: there should be some extracted items
        assertTrue(total > 0);
    }
}


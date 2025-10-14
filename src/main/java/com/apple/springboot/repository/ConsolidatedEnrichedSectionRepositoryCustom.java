package com.apple.springboot.repository;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import java.util.List;

public interface ConsolidatedEnrichedSectionRepositoryCustom {
    List<ConsolidatedEnrichedSection> findByFullTextSearch(String query);
}
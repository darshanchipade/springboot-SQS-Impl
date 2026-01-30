package com.apple.springboot.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;
import java.util.TreeMap;

@Data
@EqualsAndHashCode(callSuper = false)
public class Facets extends TreeMap<String, Object> {

    /**
     * Creates an empty facets map.
     */
    public Facets() {
        super();
    }

    /**
     * Creates a copy of another facets instance.
     */
    public Facets(Facets other) {
        super(other);
    }
}

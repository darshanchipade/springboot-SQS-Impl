package com.apple.springboot.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;
import java.util.TreeMap;

@Data
@EqualsAndHashCode(callSuper = false)
public class Facets extends TreeMap<String, Object> {

    public Facets() {
        super();
    }

    public Facets(Facets other) {
        super(other);
    }
}

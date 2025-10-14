package com.apple.springboot.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = false)
public class Facets extends HashMap<String, Object> {

    public Facets() {
        super();
    }

    public Facets(Facets other) {
        super(other);
    }
}

package com.apple.springboot.service;

public class ThrottledException extends RuntimeException {
    /**
     * Creates an exception describing a throttled upstream call.
     */
    public ThrottledException(String m) { super(m); }
    /**
     * Creates an exception that preserves the originating cause.
     */
    public ThrottledException(String m, Throwable c) { super(m, c); }
}
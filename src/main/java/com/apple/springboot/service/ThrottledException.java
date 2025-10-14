package com.apple.springboot.service;

public class ThrottledException extends RuntimeException {
    public ThrottledException(String m) { super(m); }
    public ThrottledException(String m, Throwable c) { super(m, c); }
}
package com.orderflow.orders.exception;

public class IllegalOrderStateException extends RuntimeException {

    public IllegalOrderStateException(String message) {
        super(message);
    }
}
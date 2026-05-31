package com.gamingintel.processor_service.exception;

public class AlertNotFoundException extends RuntimeException {

    public AlertNotFoundException(String gid) {
        super("Alert not found for gid: " + gid);
    }
}
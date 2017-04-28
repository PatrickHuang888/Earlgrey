package com.hxm.earlgrey.control;

/**
 * Created by hxm on 17-4-26.
 */
public class ErrorMessage {
    private String message;
    private String description;

    public ErrorMessage(String message, String description) {
        this.message = message;
        this.description = description;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
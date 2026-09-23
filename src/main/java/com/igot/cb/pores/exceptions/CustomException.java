package com.igot.cb.pores.exceptions;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public class CustomException extends RuntimeException {
    private final String code;
    private final String message;
    private final HttpStatus httpStatusCode;

    public CustomException(String code, String message, HttpStatus httpStatusCode, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.message = message;
        this.httpStatusCode = httpStatusCode;
    }
}

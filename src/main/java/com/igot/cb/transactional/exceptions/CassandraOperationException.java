package com.igot.cb.transactional.exceptions;

import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import org.springframework.http.HttpStatus;

/**
 * Custom exception class for handling errors related to Cassandra read/write operations.
 */
public class CassandraOperationException extends CustomException {

    /**
     * Constructs a new CassandraOperationException with the specified error message and cause.
     *
     * @param message The error message associated with the exception.
     * @param cause   The cause of the exception.
     */
    public CassandraOperationException(String message, Throwable cause) {
        super(Constants.ERROR, message, HttpStatus.INTERNAL_SERVER_ERROR, cause);
    }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.oasis.core.services.api.handlers;

import io.github.oasis.core.exception.OasisParseException;
import io.github.oasis.core.services.api.configs.ErrorMessages;
import io.github.oasis.core.services.api.exceptions.ErrorCodes;
import io.github.oasis.core.services.exceptions.OasisApiException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.util.WebUtils;

import java.io.Serializable;
import java.time.Instant;

/**
 * @author Isuru Weerarathna
 */
@ControllerAdvice
public class OasisErrorHandler extends ResponseEntityExceptionHandler {

    private static final HttpHeaders ERROR_HEADERS = new HttpHeaders();

    private final ErrorMessages errorMessages;

    static {
        ERROR_HEADERS.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
    }

    public OasisErrorHandler(ErrorMessages errorMessages) {
        this.errorMessages = errorMessages;
    }

    @ExceptionHandler(value = { OasisApiException.class })
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    protected ResponseEntity<ErrorObject> handleValidationError(OasisApiException ex, WebRequest request) {
        ErrorObject errorObject = new ErrorObject();
        errorObject.setTimestamp(Instant.now().toString());
        errorObject.setStatus(ex.getStatusCode());
        errorObject.setErrorCode(ex.getErrorCode());
        errorObject.setErrorCodeDescription(errorMessages.getErrorMessage(ex.getErrorCode()));
        errorObject.setMessage(ex.getMessage());
        if (request instanceof ServletWebRequest) {
            errorObject.setPath(((ServletWebRequest) request).getRequest().getServletPath());
        } else {
            errorObject.setPath(request.getContextPath());
        }
        request.setAttribute(WebUtils.ERROR_EXCEPTION_ATTRIBUTE, ex, WebRequest.SCOPE_REQUEST);
        return new ResponseEntity<>(errorObject, ERROR_HEADERS, HttpStatus.valueOf(ex.getStatusCode()));
    }

    @ExceptionHandler(value = { OasisParseException.class })
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    protected ResponseEntity<ErrorObject> handleParseError(OasisParseException ex, WebRequest request) {
        ErrorObject errorObject = new ErrorObject();
        String errorCode = StringUtils.defaultIfEmpty(ex.getErrorCode(), ErrorCodes.ELEMENT_SPEC_INVALID);
        errorObject.setTimestamp(Instant.now().toString());
        errorObject.setStatus(HttpStatus.BAD_REQUEST.value());
        errorObject.setErrorCode(errorCode);
        errorObject.setErrorCodeDescription(errorMessages.getErrorMessage(errorCode));
        errorObject.setMessage(ex.getMessage());
        if (request instanceof ServletWebRequest) {
            errorObject.setPath(((ServletWebRequest) request).getRequest().getServletPath());
        } else {
            errorObject.setPath(request.getContextPath());
        }
        request.setAttribute(WebUtils.ERROR_EXCEPTION_ATTRIBUTE, ex, WebRequest.SCOPE_REQUEST);
        return new ResponseEntity<>(errorObject, ERROR_HEADERS, HttpStatus.valueOf(HttpStatus.BAD_REQUEST.value()));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder(toBuilder = true)
    public static class ErrorObject implements Serializable {
        private String timestamp;
        private int status;
        private String errorCode;
        private String errorCodeDescription;
        private String message;
        private String path;
    }

}

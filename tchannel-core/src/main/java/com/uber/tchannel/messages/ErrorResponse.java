/*
 * Copyright (c) 2015 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.uber.tchannel.messages;

import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.frames.FrameType;

public final class ErrorResponse extends ResponseMessage {

    private final ErrorType errorType;
    private final String message;
    private final long id;
    private Throwable throwable;

    public ErrorResponse(long id, ErrorType errorType, String message) {
        this.id = id;
        this.errorType = errorType;
        this.message = message;
        this.type = FrameType.Error;
        this.throwable = null;
    }

    public ErrorResponse(long id, ErrorType errorType, Throwable throwable) {
        this(id, errorType, throwable.getMessage());
        this.throwable = throwable;
    }

    @Override
    public long getId() {
        return this.id;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return String.format(
            "<%s id=%s errorType=%s message=%s>",
            this.getClass().getSimpleName(),
            this.id,
            this.errorType,
            this.message
        );
    }

    @Override
    public void release() {}
}

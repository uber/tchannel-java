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

package com.uber.tchannel.errors;

import com.uber.tchannel.tracing.Trace;

public class FatalProtocolError extends Exception implements ProtocolError {
    private final long id;
    private final ErrorType errorType = ErrorType.FatalProtocolError;
    private final Trace trace;
    private final String message;

    public FatalProtocolError(long id, Trace trace, String message) {
        this.id = id;
        this.trace = trace;
        this.message = message;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public ErrorType getErrorType() {
        return errorType;
    }

    @Override
    public Trace getTrace() {
        return trace;
    }

    @Override
    public String getErrorMessage() {
        return message;
    }
}

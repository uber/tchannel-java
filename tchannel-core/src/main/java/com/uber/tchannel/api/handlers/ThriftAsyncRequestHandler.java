/*
 * Copyright (c) 2016 Uber Technologies, Inc.
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

package com.uber.tchannel.api.handlers;

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;

import java.util.concurrent.ExecutionException;

/**
 * Specialized async handler for thrift calls. Like the AsyncRequestHandler it also allows requests
 * to be handle asynchronously by letting the actual handler return a future.
 */
public abstract class ThriftAsyncRequestHandler<T, U> implements AsyncRequestHandler {
    @Override
    public Response handle(Request request) {
        try {
            return this.handleAsync(request).get();
        } catch (InterruptedException | ExecutionException ex) {
            // Cannot do much better with this exception so propogate the exception.
            throw new RuntimeException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ListenableFuture<? extends Response> handleAsync(Request request) {
        return handleImpl((ThriftRequest<T>)request);
    }

    /**
     * Main handle method for ThriftAsyncRequestHandler that return a ListenableFuture scoped down
     * to ThriftResponse.  This method should not make any blocking calls so that the calling thread
     * is reliquished quickly.
     *
     * @param request ThriftRequest to handle
     * @return A ListenableFuture representing the result of thrift request handling.
     */
    public abstract ListenableFuture<ThriftResponse<U>> handleImpl(ThriftRequest<T> request);
}

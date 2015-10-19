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

package com.uber.tchannel.handlers;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.errors.BadRequestError;
import com.uber.tchannel.errors.BusyError;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.messages.ErrorMessage;
import com.uber.tchannel.schemes.JSONSerializer;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.Serializer;
import com.uber.tchannel.schemes.ThriftSerializer;
import com.uber.tchannel.tracing.Trace;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestRouter extends SimpleChannelInboundHandler<RawRequest> {

    private final Map<String, ? extends RequestHandler> requestHandlers;
    private final Serializer serializer;
    private final AtomicInteger queuedRequests = new AtomicInteger(0);
    private final int maxQueuedRequests;
    private final ListeningExecutorService listeningExecutorService;

    public RequestRouter(Map<String, RequestHandler> requestHandlers,
                         ExecutorService executorService, int maxQueuedRequests) {
        this.requestHandlers = requestHandlers;
        this.serializer = new Serializer(new HashMap<ArgScheme, Serializer.SerializerInterface>() {
            {
                put(ArgScheme.JSON, new JSONSerializer());
                put(ArgScheme.THRIFT, new ThriftSerializer());
            }
        }
        );
        this.listeningExecutorService = MoreExecutors.listeningDecorator(executorService);
        this.maxQueuedRequests = maxQueuedRequests;
    }

    @Override
    protected void messageReceived(final ChannelHandlerContext ctx, final RawRequest rawRequest) throws Exception {

        final ArgScheme argScheme = ArgScheme.toScheme(
                rawRequest.getTransportHeaders().get(TransportHeaders.ARG_SCHEME_KEY)
        );

        if (argScheme == null) {
            throw new BadRequestError(
                    "Missing `Arg Scheme` header",
                    new Trace(0, 0, 0, (byte) 0),
                    rawRequest.getId()
            );
        }

        /** If the current queued request count is greater than the expected queued
         * request count then send a busy error so that the caller can try a different
         * node.
         */
        if (queuedRequests.get() >= maxQueuedRequests) {
            throw new BusyError(
                    "Busy",
                    new Trace(0, 0, 0, (byte) 0),
                    rawRequest.getId()
            );
        }

        // Get the endpoint. The assumption over here is that endpoints are
        // always going to to utf-8 encoded.
        String endpoint = rawRequest.getArg1().toString(CharsetUtil.UTF_8);

        // Get handler for this method
        RequestHandler handler = this.requestHandlers.get(endpoint);

        if (handler == null) {
            throw new RuntimeException(String.format("No handler for %s", endpoint));
        }

        // Increment the number of queued requests.
        queuedRequests.getAndIncrement();

        // Handle the request in a separate thread and get a future to it
        ListenableFuture<RawResponse> responseFuture = listeningExecutorService.submit(
                new CallableHandler(handler, rawRequest));

        Futures.addCallback(responseFuture, new FutureCallback<RawResponse>() {
            @Override
            public void onSuccess(RawResponse response) {

                // Since the request was handled, decrement the queued requests count
                queuedRequests.decrementAndGet();
                ctx.writeAndFlush(response);
            }

            @Override
            public void onFailure(Throwable throwable) {
                queuedRequests.decrementAndGet();

                // TODO better interface for sending errors
                ErrorMessage error = new ErrorMessage(
                        rawRequest.getId(),
                        ErrorType.BadRequest,
                        new Trace(0, 0, 0, (byte) 0x00),
                        throwable.getMessage());
                ctx.writeAndFlush(error);
            }
        });
    }

    private class CallableHandler implements Callable<RawResponse> {
        private final RawRequest request;
        private final RequestHandler handler;

        public CallableHandler(RequestHandler handler, RawRequest request) {
            this.handler = handler;
            this.request = request;
        }

        @Override
        public RawResponse call() throws Exception {
            RawResponse response = handler.handle(request);
            return response;
        }
    }
}

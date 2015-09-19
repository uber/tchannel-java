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
import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.DefaultRequestHandler;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.errors.BadRequestError;
import com.uber.tchannel.errors.BusyError;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.JSONSerializer;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.Serializer;
import com.uber.tchannel.schemes.ThriftSerializer;
import com.uber.tchannel.tracing.Trace;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestRouter extends SimpleChannelInboundHandler<RawRequest> {

    private final Map<String, ? extends DefaultRequestHandler> requestHandlers;
    private final Serializer serializer;
    private final AtomicInteger queuedRequests = new AtomicInteger(0);
    private final int maxQueuedRequests;
    private final ListeningExecutorService listeningExecutorService;

    public RequestRouter(Map<String, DefaultRequestHandler> requestHandlers,
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
                rawRequest.getTransportHeaders().getOrDefault(TransportHeaders.ARG_SCHEME_KEY, null)
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

        // Increment the number of queued requests.
        queuedRequests.getAndIncrement();

        // arg1
        String endpoint = this.serializer.decodeEndpoint(rawRequest);

        // Get handler for this method
        DefaultRequestHandler<?, ?> handler = this.requestHandlers.get(endpoint);

        if (handler == null) {
            throw new RuntimeException(String.format("No handler for %s", endpoint));
        }

        // arg2
        Map<String, String> applicationHeaders = this.serializer.decodeHeaders(rawRequest);

        // arg3
        Object body = this.serializer.decodeBody(rawRequest, handler.getRequestType());

        // transform request into form the handler expects
        Request<?> request = new Request.Builder<>(body, rawRequest.getService(), endpoint)
                .setHeaders(applicationHeaders)
                .setTransportHeaders(rawRequest.getTransportHeaders())
                .build();

        // Handle the request in a separate thread and get a future to it
        ListenableFuture<Response<?>> responseFuture = listeningExecutorService.submit(
                new CallableHandler(handler, request));

        Futures.addCallback(responseFuture, new FutureCallback<Response<?>>() {
            @Override
            public void onSuccess(Response<?> response) {

                // Since the request was handled, decrement the queued requests count
                queuedRequests.decrementAndGet();

                RawResponse rawResponse = new RawResponse(
                        rawRequest.getId(),
                        response.getResponseCode(),
                        rawRequest.getTransportHeaders(),
                        serializer.encodeEndpoint(response.getEndpoint(), argScheme),
                        serializer.encodeHeaders(response.getHeaders(), argScheme),
                        serializer.encodeBody(response.getBody(), argScheme)
                );

                ctx.writeAndFlush(rawResponse);
            }

            @Override
            public void onFailure(Throwable throwable) {
                // TODO handle the failure case
            }
        });
    }

    private class CallableHandler implements Callable<Response<?>> {
        private final Request<?> request;
        private final DefaultRequestHandler<?, ?> handler;

        public CallableHandler(DefaultRequestHandler<?, ?> handler, Request<?> request) {
            this.handler = handler;
            this.request = request;
        }

        @Override
        public Response<?> call() throws Exception {
            Response<?> response = handler.handle((Request) request);
            return response;
        }
    }
}

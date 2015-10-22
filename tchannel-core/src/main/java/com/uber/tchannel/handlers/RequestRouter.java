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
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.JSONSerializer;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.ResponseMessage;
import com.uber.tchannel.schemes.Serializer;
import com.uber.tchannel.schemes.ThriftSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.uber.tchannel.frames.ErrorFrame.sendError;

public class RequestRouter extends SimpleChannelInboundHandler<RawRequest> {

    private final TChannel topChannel;

    private final Serializer serializer;
    private final AtomicInteger queuedRequests = new AtomicInteger(0);
    private final int maxQueuedRequests;
    private final ListeningExecutorService listeningExecutorService;

    public RequestRouter(TChannel topChannel,
                         ExecutorService executorService, int maxQueuedRequests) {
        this.topChannel = topChannel;
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

    private RequestHandler getRequestHandler(String service, String endpoint) {
        SubChannel subChannel = topChannel.getSubChannel(service);
        RequestHandler handler = null;

        if (subChannel != null) {
            handler = subChannel.getRequestHandler(endpoint);
        }

        return handler;
    }

    @Override
    protected void messageReceived(final ChannelHandlerContext ctx, final RawRequest rawRequest) throws Exception {

        final ArgScheme argScheme = ArgScheme.toScheme(
                rawRequest.getTransportHeaders().get(TransportHeaders.ARG_SCHEME_KEY)
        );

        if (argScheme == null) {
            sendError(ErrorType.BadRequest,
                "Expected incoming call to have \"as\" header set",
                rawRequest, ctx);
            return;
        }

        final String service = rawRequest.getService();
        if (service == null || service.isEmpty()) {
            sendError(ErrorType.BadRequest,
                "Expected incoming call to have serviceName",
                rawRequest, ctx);
            return;
        }

        /** If the current queued request count is greater than the expected queued
         * request count then send a busy error so that the caller can try a different
         * node.
         */
        if (queuedRequests.get() >= maxQueuedRequests) {
            sendError(ErrorType.Busy, "Service is busy", rawRequest, ctx);
            return;
        }

        // Get the endpoint. The assumption over here is that endpoints are
        // always going to to utf-8 encoded.
        String endpoint = rawRequest.getArg1().toString(CharsetUtil.UTF_8);
        if (endpoint == null || endpoint.isEmpty()) {
            sendError(ErrorType.BadRequest,
                "Expected incoming call to have endpoint",
                rawRequest, ctx);
            return;
        }

        // Get handler for this method
        RequestHandler handler = this.getRequestHandler(service, endpoint);
        if (handler == null) {
            sendError(ErrorType.BadRequest,
                "Invalid handler function",
                rawRequest, ctx);
            return;
        }

        // Increment the number of queued requests.
        queuedRequests.getAndIncrement();

        // Handle the request in a separate thread and get a future to it
        ListenableFuture<ResponseMessage> responseFuture = listeningExecutorService.submit(
                new CallableHandler(handler, rawRequest));

        Futures.addCallback(responseFuture, new FutureCallback<ResponseMessage>() {
            @Override
            public void onSuccess(ResponseMessage response) {

                // Since the request was handled, decrement the queued requests count
                queuedRequests.decrementAndGet();
                ctx.writeAndFlush(response);
            }

            @Override
            public void onFailure(Throwable throwable) {
                queuedRequests.decrementAndGet();
                sendError(ErrorType.BadRequest,
                    "Failed to handle the request: " + throwable.getMessage(),
                    rawRequest, ctx);
                return;
            }
        });
    }

    private class CallableHandler implements Callable<ResponseMessage> {
        private final RawRequest request;
        private final RequestHandler handler;

        public CallableHandler(RequestHandler handler, RawRequest request) {
            this.handler = handler;
            this.request = request;
        }

        @Override
        public ResponseMessage call() throws Exception {
            ResponseMessage response = handler.handle(request);
            return response;
        }
    }
}

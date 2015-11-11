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
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.uber.tchannel.frames.ErrorFrame.sendError;

public class RequestRouter extends SimpleChannelInboundHandler<Request> {

    private final TChannel topChannel;
    private AtomicBoolean destroyed = new AtomicBoolean(false);

    private final ListeningExecutorService listeningExecutorService;

    private final List<ResponseMessage> responseQueue = new LinkedList<ResponseMessage>();

    public RequestRouter(TChannel topChannel, ExecutorService executorService) {
        this.topChannel = topChannel;
        this.listeningExecutorService = MoreExecutors.listeningDecorator(executorService);
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
    protected void messageReceived(final ChannelHandlerContext ctx, final Request request) {

        // There is nothing to do if the connection is already distroyed.
        if (destroyed.get()) {
            request.release();
            return;
        }

        final ArgScheme argScheme = ArgScheme.toScheme(
                request.getTransportHeaders().get(TransportHeaders.ARG_SCHEME_KEY)
        );

        if (argScheme == null) {
            sendError(ErrorType.BadRequest,
                "Expected incoming call to have \"as\" header set",
                request, ctx);
            return;
        }

        final String service = request.getService();
        if (service == null || service.isEmpty()) {
            sendError(ErrorType.BadRequest,
                "Expected incoming call to have serviceName",
                request, ctx);
            return;
        }

        // Get the endpoint. The assumption over here is that endpoints are
        // always going to to utf-8 encoded.
        String endpoint = request.getArg1().toString(CharsetUtil.UTF_8);
        if (endpoint == null || endpoint.isEmpty()) {
            sendError(ErrorType.BadRequest,
                "Expected incoming call to have endpoint",
                request, ctx);
            return;
        }

        // Get handler for this method
        RequestHandler handler = this.getRequestHandler(service, endpoint);
        if (handler == null) {
            sendError(ErrorType.BadRequest,
                "Invalid handler function",
                request, ctx);
            return;
        }

        // Handle the request in a separate thread and get a future to it
        ListenableFuture<ResponseMessage> responseFuture = listeningExecutorService.submit(
                new CallableHandler(handler, request));

        Futures.addCallback(responseFuture, new FutureCallback<ResponseMessage>() {
            @Override
            public void onSuccess(ResponseMessage response) {
                // TODO: aggregate the flush
                if (!ctx.channel().isWritable()) {
                    synchronized (responseQueue) {
                        responseQueue.add(response);
                    }
                } else {
                    ctx.writeAndFlush(response);
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                // TODO: log the exception

                sendError(ErrorType.BadRequest,
                    "Failed to handle the request: " + throwable.getMessage(),
                    request, ctx);
                return;
            }
        });
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (!ctx.channel().isWritable()) {
            return;
        }

        synchronized (responseQueue) {
            while (!responseQueue.isEmpty() && ctx.channel().isWritable()) {
                ResponseMessage res = responseQueue.remove(0);
                ctx.writeAndFlush(res);
            }
        }
    }

    public void clean() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
    }

    private class CallableHandler implements Callable<ResponseMessage> {
        private final Request request;
        private final RequestHandler handler;

        public CallableHandler(RequestHandler handler, Request request) {
            this.handler = handler;
            this.request = request;
        }

        @Override
        public ResponseMessage call() throws Exception {
            ResponseMessage response = handler.handle(request);
            request.release();
            return response;
        }
    }
}

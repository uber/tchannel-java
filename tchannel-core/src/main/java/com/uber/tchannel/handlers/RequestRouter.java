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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.AsyncRequestHandler;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.errors.ProtocolError;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.tracing.Tracing;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.opentracing.Span;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.uber.tchannel.frames.ErrorFrame.sendError;

public class RequestRouter extends SimpleChannelInboundHandler<Request> {

    private static final Logger logger = LoggerFactory.getLogger(RequestRouter.class);

    private final @NotNull TChannel topChannel;

    private final @NotNull ListeningExecutorService listeningExecutorService;

    private final @NotNull AtomicBoolean busy = new AtomicBoolean(false);

    private final @NotNull ConcurrentLinkedQueue<Response> responseQueue = new ConcurrentLinkedQueue<>();

    public RequestRouter(@NotNull TChannel topChannel, @NotNull ExecutorService executorService) {
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
    protected void channelRead0(final ChannelHandlerContext ctx, final Request request) {

        // There is nothing to do if the connection is already destroyed.
        if (!ctx.channel().isActive()) {
            request.release();
            logger.warn("drop request when channel is inActive");
            return;
        }

        if (request.getArgScheme() == null) {
            sendError(ErrorType.BadRequest, "Expected incoming call to have \"as\" header set", request, ctx);
            return;
        }

        final String service = request.getService();
        if (service == null || service.isEmpty()) {
            sendError(ErrorType.BadRequest, "Expected incoming call to have serviceName", request, ctx);
            return;
        }

        // Get the endpoint. The assumption over here is that endpoints are
        // always going to to utf-8 encoded.
        String endpoint = request.getEndpoint();
        if (endpoint == null || endpoint.isEmpty()) {
            sendError(ErrorType.BadRequest, "Expected incoming call to have endpoint", request, ctx);
            return;
        }

        // Get handler for this method
        RequestHandler handler = this.getRequestHandler(service, endpoint);
        if (handler == null) {
            handler = topChannel.getDefaultUserHandler();
        }
        if (handler == null) {
            sendError(
                ErrorType.BadRequest,
                "No handler function for service:endpoint=" + service + ':' + endpoint,
                request,
                ctx
            );
            return;
        }

        ListenableFuture<? extends Response> responseFuture;
        try {
            // In case of an AsyncRequestHandler there's no need to submit a task on the executor.
            // It does require a down-cast to AsyncRequestHandler.
            responseFuture = handler instanceof AsyncRequestHandler
                ? sendRequestToAsyncHandler((AsyncRequestHandler) handler, request)
                : listeningExecutorService.submit(new CallableHandler(handler, topChannel, request));
        } catch (Throwable re) {
            request.release();
            responseFuture = Futures.immediateFailedFuture(re);
        }

        Futures.addCallback(responseFuture, new FutureCallback<Response>() {

            @Override
            public void onSuccess(Response response) {
                if (ctx.channel().isActive()) {
                    responseQueue.offer(response);
                    ctx.channel().eventLoop().execute(new Runnable() {
                        @Override
                        public void run() {
                            sendResponse(ctx);
                        }
                    });
                } else {
                    response.release();
                }
            }

            @Override
            public void onFailure(@NotNull Throwable throwable) {
                logger.error("Failed to handle the request due to exception.", throwable);

                ErrorType errorType = null;
                if (throwable instanceof ProtocolError) {
                    ProtocolError protocolError = (ProtocolError) throwable;
                    errorType = protocolError.getErrorType();
                }

                if (errorType == null) {
                    errorType = ErrorType.UnexpectedError;
                }

                sendError(
                    errorType, "Failed to handle the request: " + throwable.getMessage(), request, ctx
                );
            }

        }, listeningExecutorService);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        sendResponse(ctx);
    }

    protected void sendResponse(ChannelHandlerContext ctx) {

        if (!busy.compareAndSet(false, true)) {
            return;
        }

        Channel channel = ctx.channel();
        try {
            boolean flush = false;
            while (channel.isWritable()) {
                Response res = responseQueue.poll();
                if (res == null) {
                    break;
                }

                channel.write(res, channel.voidPromise());
                flush = true;
            }

            if (flush) {
                channel.flush();
            }
        } finally {
            busy.set(false);
        }

        // in case there are new response added
        if (channel.isWritable() && !responseQueue.isEmpty()) {
            sendResponse(ctx);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        // clean up the queue
        while (!responseQueue.isEmpty()) {
            responseQueue.poll().release();
        }
    }

    private ListenableFuture<? extends Response> sendRequestToAsyncHandler(
        final AsyncRequestHandler asyncHandler, final Request request
    ) {
        // span used to trace this request
        // Tracer and TracingContext are only present when the channel is created with them
        // therefore can be null.
        final Span span = topChannel.getTracer() == null
            ? null
            : Tracing.startInboundSpan(request, topChannel.getTracer(), topChannel.getTracingContext());

        ListenableFuture<? extends Response> responseFuture = asyncHandler.handleAsync(request);

        // Add callback handlers that close out the tracing span and then proxy the response.
        Futures.addCallback(responseFuture, new FutureCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                closeRequestAndSpan();
            }

            @Override
            public void onFailure(@NotNull Throwable e) {
                if (span != null) {
                    span.log(ImmutableMap.of("exception", e));
                }
                closeRequestAndSpan();
            }

            private void closeRequestAndSpan() {
                request.release();
                if (span != null) {
                    span.finish();
                }
            }
        }, listeningExecutorService); // execute the callback asynchronously, not on the thread that resolves the future

        if (span != null) { // if we pushed something on tracing context stack in Tracing.startInboundSpan(...)
            topChannel.getTracingContext().popSpan(); // then pop it
        }

        return responseFuture;
    }

    private static class CallableHandler implements Callable<Response> {

        private final Request request;
        private final TChannel topChannel;
        private final RequestHandler handler;

        CallableHandler(RequestHandler handler, TChannel topChannel, Request request) {
            this.handler = handler;
            this.topChannel = topChannel;
            this.request = request;
        }

        @Override
        public Response call() {
            if (topChannel.getTracer() == null) {
                return callWithoutTracing();
            }
            Span span = Tracing.startInboundSpan(request, topChannel.getTracer(), topChannel.getTracingContext());
            try {
                return callWithoutTracing();
            } catch (Throwable t) {
                span.log(ImmutableMap.of("exception", t));
                throw t;
            } finally {
                span.finish();
                topChannel.getTracingContext().clear();
            }
        }

        private Response callWithoutTracing() {
            try {
                return handler.handle(request);
            } finally {
                request.release();
            }
        }

    }

}

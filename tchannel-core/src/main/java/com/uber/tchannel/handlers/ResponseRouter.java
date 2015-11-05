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

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.messages.ErrorResponse;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.messages.ResponseMessage;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ResponseRouter extends SimpleChannelInboundHandler<ResponseMessage> {

    private final HashedWheelTimer timer;

    private final Map<Long, OutRequest> requestMap = new ConcurrentHashMap<>();

    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private ChannelHandlerContext ctx;

    public ResponseRouter(HashedWheelTimer timer) {
        this.timer = timer;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    public <V> ListenableFuture<V> expectResponse(Request request) {
        int messageId = idGenerator.incrementAndGet();
        request.setId(messageId);
        TFuture<V> future = TFuture.create();
        OutRequest<V> outRequest = new OutRequest<V>(request, future);
        send(outRequest);
        return future;
    }

    protected <V> boolean send(OutRequest<V> outRequest) {
        if (!outRequest.shouldRetry()) {
            messageReceived(ctx, outRequest.getLastError());
            return false;
        }

        Request request = outRequest.getRequest();
        this.requestMap.put(request.getId(), outRequest);
        ctx.writeAndFlush(request);
        setTimer(outRequest);

        return true;
    }

    protected <V> void setTimer(final OutRequest<V> outRequest) {
        final long start = System.currentTimeMillis();
        Timeout timeout = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                messageReceived(ctx, new ErrorResponse(
                    outRequest.getRequest().getId(),
                    ErrorType.Timeout,
                    String.format("Request timeout after %dms", System.currentTimeMillis() - start)));
            }
        }, outRequest.getRequest().getTimeout(), TimeUnit.MILLISECONDS);

        outRequest.setTimeout(timeout);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected void messageReceived(ChannelHandlerContext ctx, ResponseMessage response) {

        OutRequest outRequest = this.requestMap.remove(response.getId());
        if (outRequest == null) {
            response.release();
            // TODO logging
            return;
        }

        outRequest.getTimeout().cancel();
        TFuture future = outRequest.getFuture();
        Request request = outRequest.getRequest();

        ArgScheme argScheme = request.getArgScheme();
        if (argScheme == null) {
            if (request instanceof JsonRequest) {
                argScheme = ArgScheme.JSON;
            } else if (request instanceof ThriftRequest) {
                argScheme = ArgScheme.THRIFT;
            } else {
                argScheme = ArgScheme.RAW;
            }
        }

        Response res;
        if (response.isError()) {
            outRequest.setLastError((ErrorResponse) response);
            if (send(outRequest)) {
                return;
            }

            res = Response.build(argScheme, (ErrorResponse) response);
        } else {
            res = (Response) response;
        }

        // release the request
        request.release();
        switch (argScheme) {
            case RAW:
                ((TFuture<RawResponse>)future).set((RawResponse) res);
                break;
            case JSON:
                ((TFuture<JsonResponse>)future).set((JsonResponse) res);
                break;
            case THRIFT:
                ((TFuture<ThriftResponse>)future).set((ThriftResponse) res);
                break;
            default:
                ((TFuture<RawResponse>)future).set((RawResponse) res);
                break;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // TODO: logging instead of print
        TChannelUtilities.PrintException(cause);
    }
}

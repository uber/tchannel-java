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
import com.google.common.util.concurrent.SettableFuture;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.schemes.ErrorResponse;
import com.uber.tchannel.schemes.JsonRequest;
import com.uber.tchannel.schemes.JsonResponse;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.Request;
import com.uber.tchannel.schemes.Response;
import com.uber.tchannel.schemes.ResponseMessage;
import com.uber.tchannel.schemes.ThriftRequest;
import com.uber.tchannel.schemes.ThriftResponse;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ResponseRouter extends SimpleChannelInboundHandler<ResponseMessage> {

//    private final Map<Long, SettableFuture<Response>> messageMap = new ConcurrentHashMap<>();s

    private final Map<Long, Object> messageMap = new ConcurrentHashMap<>();
    private final Map<Long, Request> requestMap = new ConcurrentHashMap<>();

    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private ChannelHandlerContext ctx;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

//    public <V> ListenableFuture<V> expectResponse(Request request) throws InterruptedException {
//        int messageId = idGenerator.incrementAndGet();
//        request.setId(messageId);
//        SettableFuture<V> future = SettableFuture.create();
//        this.messageMap.put(request.getId(), future);
//        ctx.writeAndFlush(request);
//        return future;
//    }
//
//    @Override
//    protected void messageReceived(ChannelHandlerContext ctx, Response response) throws Exception {
//        SettableFuture<Response> future = this.messageMap.remove(response.getId());
//        response.release();
//        future.set(response);
//    }

    public <V> ListenableFuture<V> expectResponse(Request request) throws InterruptedException {
        int messageId = idGenerator.incrementAndGet();
        request.setId(messageId);
        SettableFuture<V> future = SettableFuture.create();
        this.messageMap.put(request.getId(), future);
        this.requestMap.put(request.getId(), request);
        ctx.writeAndFlush(request);
        return future;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, ResponseMessage response) throws Exception {

        Object future = this.messageMap.remove(response.getId());
        if (future == null) {
            return;
        }

        // TODO: handle timeout here
        // TODO better interface?
        Request request = this.requestMap.remove(response.getId());
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

        request.release();

        Response res;
        if (response.isError()) {
            res = Response.build(argScheme, (ErrorResponse) response);
        } else {
            res = (Response) response;
        }

        switch (argScheme) {
            case RAW:
                ((SettableFuture<RawResponse>)future).set((RawResponse) res);
                break;
            case JSON:
                ((SettableFuture<JsonResponse>)future).set((JsonResponse) res);
                break;
            case THRIFT:
                ((SettableFuture<ThriftResponse>)future).set((ThriftResponse) res);
                break;
            default:
                ((SettableFuture<Response>)future).set((Response) res);
                break;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // TODO: logging instead of print
        TChannelUtilities.PrintException(cause);
    }
}

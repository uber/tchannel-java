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

import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.JSONSerializer;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.Serializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ResponseRouter extends SimpleChannelInboundHandler<RawResponse> {

    private final Map<Long, ResponsePromise> messageMap = new HashMap<>();
    private final Serializer serializer = new Serializer(new HashMap<ArgScheme, Serializer.SerializerInterface>() {
        {
            put(ArgScheme.JSON, new JSONSerializer());
        }
    });
    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private ChannelHandlerContext ctx;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    public <T, U> Promise<Response<T>> expectResponse(Request<U> request,
                                                      Class<T> responseType) throws InterruptedException {

        String as = request.getTransportHeaders().get(TransportHeaders.ARG_SCHEME_KEY);

        ArgScheme argScheme = ArgScheme.toScheme(as);

        RawRequest rawRequest = new RawRequest(
                idGenerator.getAndIncrement(),
                request.getService(),
                request.getTransportHeaders(),
                serializer.encodeEndpoint(request.getEndpoint(), argScheme),
                serializer.encodeHeaders(request.getHeaders(), argScheme),
                serializer.encodeBody(request.getBody(), argScheme)
        );

        Promise<Response<T>> responsePromise = new DefaultPromise<>(ctx.executor());
        this.messageMap.put(rawRequest.getId(), new ResponsePromise<>(responsePromise, responseType));
        ctx.writeAndFlush(rawRequest).await();
        return responsePromise;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, RawResponse rawResponse) throws Exception {

        ResponsePromise<?> responsePromise = this.messageMap.remove(rawResponse.getId());

        Response<?> response = new Response.Builder<>(
                this.serializer.decodeBody(rawResponse, responsePromise.getPromiseType()))
                .setEndpoint(this.serializer.decodeEndpoint(rawResponse))
                .setHeaders(this.serializer.decodeHeaders(rawResponse))
                .build();

        responsePromise.getPromise().setSuccess((Response) response);

    }

    private class ResponsePromise<T> {
        private final Promise<Response<T>> promise;
        private final Class<T> promiseType;

        public ResponsePromise(Promise<Response<T>> promise, Class<T> promiseType) {
            this.promise = promise;
            this.promiseType = promiseType;
        }

        public Promise<Response<T>> getPromise() {
            return promise;
        }

        public Class<T> getPromiseType() {
            return promiseType;
        }
    }

}

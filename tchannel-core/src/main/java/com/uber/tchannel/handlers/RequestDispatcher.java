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
import com.uber.tchannel.api.RequestHandler;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.DefaultRawRequestHandler;
import com.uber.tchannel.schemes.JSONArgScheme;
import com.uber.tchannel.schemes.JSONRequest;
import com.uber.tchannel.schemes.JSONRequestHandler;
import com.uber.tchannel.schemes.JSONResponse;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.Map;
import java.util.concurrent.Callable;

public class RequestDispatcher extends SimpleChannelInboundHandler<Request> {

    private final Map<String, RequestHandler> requestHandlers;

    public RequestDispatcher(Map<String, RequestHandler> requestHandlers) {
        this.requestHandlers = requestHandlers;
    }

    @Override
    protected void messageReceived(final ChannelHandlerContext ctx, Request request) throws Exception {

        ArgScheme argScheme = ArgScheme.fromString(
                request.getTransportHeaders().get(TransportHeaders.ARG_SCHEME_KEY)
        );

        if (argScheme == null) {
            throw new RuntimeException("Missing `Arg Scheme` header");
        }

        RawRequest rawRequest = ((RawRequest) request);
        switch (argScheme) {
            case RAW:
                RawResponse rawResponse = new DefaultRawRequestHandler().handle(rawRequest);
                ctx.writeAndFlush(rawResponse);
                break;
            case JSON:
                // arg1
                String method = JSONArgScheme.decodeMethod(rawRequest);

                // arg2
                Map<String, String> applicationHeaders = JSONArgScheme.decodeApplicationHeaders(rawRequest);

                // arg3
                final JSONRequestHandler handler = (JSONRequestHandler) this.requestHandlers.get(method);

                // deserialize object
                Object body = JSONArgScheme.decodeBody(rawRequest, handler.getRequestType());

                // transform request into form the handler expects
                final JSONRequest jsonRequest = new JSONRequest(
                        request.getId(),
                        request.getService(),
                        request.getTransportHeaders(),
                        method,
                        applicationHeaders,
                        body
                );

                // Handle the request
                Future<JSONResponse> f = ctx.executor().submit(new Callable<JSONResponse>() {
                    @Override
                    public JSONResponse call() throws Exception {
                        return (JSONResponse) handler.handle(jsonRequest);
                    }
                });

                // Write the response back when done
                f.addListener(new GenericFutureListener<Future<? super JSONResponse>>() {
                    @Override
                    public void operationComplete(Future<? super JSONResponse> future) throws Exception {
                        // serialize the object
                        RawResponse rawResponse = JSONArgScheme.encodeResponse(
                                (JSONResponse) future.get(),
                                handler.getResponseType()
                        );

                        // write and flush
                        ctx.writeAndFlush(rawResponse);
                    }
                });

                break;
            case THRIFT:
            case HTTP:
            case STREAMING_THRIFT:
            default:
                break;
        }
    }
}

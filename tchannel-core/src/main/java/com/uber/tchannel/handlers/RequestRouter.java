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

import com.uber.tchannel.api.RequestHandler;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.JSONArgScheme;
import com.uber.tchannel.schemes.JSONRequest;
import com.uber.tchannel.schemes.JSONRequestHandler;
import com.uber.tchannel.schemes.JSONResponse;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawRequestHandler;
import com.uber.tchannel.schemes.RawResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;

public class RequestRouter extends SimpleChannelInboundHandler<RawRequest> {

    private final Map<String, ? extends RequestHandler> requestHandlers;
    private final RawRequestHandler rawRequestHandler;

    public RequestRouter(Map<String, RequestHandler> requestHandlers, RawRequestHandler rawRequestHandler) {
        this.requestHandlers = requestHandlers;
        this.rawRequestHandler = rawRequestHandler;
    }

    protected void handleRaw(ChannelHandlerContext ctx, RawRequest request) {
        RawResponse rawResponse = this.rawRequestHandler.handle(request);
        ctx.writeAndFlush(rawResponse);
    }

    protected void handleJSON(ChannelHandlerContext ctx, RawRequest request) {
        // arg1
        String method = JSONArgScheme.decodeMethod(request);

        // Get handler for this method
        final JSONRequestHandler<?, ?> handler = (JSONRequestHandler<?, ?>) this.requestHandlers.get(method);

        // arg2
        Map<String, String> applicationHeaders = JSONArgScheme.decodeApplicationHeaders(request);

        // arg3
        Object body = JSONArgScheme.decodeBody(request, handler.getRequestType());

        // transform request into form the handler expects
        JSONRequest jsonRequest = new JSONRequest<>(
                request.getId(),
                request.getService(),
                request.getTransportHeaders(),
                method,
                applicationHeaders,
                body
        );

        // Handle the request
        JSONResponse<?> jsonResponse = handler.handle(jsonRequest);
        RawResponse response = JSONArgScheme.encodeResponse(jsonResponse, handler.getResponseType());
        ctx.writeAndFlush(response);
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, RawRequest request) throws Exception {

        ArgScheme argScheme = ArgScheme.fromString(request.getTransportHeaders().get(TransportHeaders.ARG_SCHEME_KEY));

        if (argScheme == null) {
            throw new RuntimeException("Missing `Arg Scheme` header");
        }

        switch (argScheme) {
            case RAW:
                this.handleRaw(ctx, request);
                break;
            case JSON:
                this.handleJSON(ctx, request);
                break;
            case THRIFT:
            case HTTP:
            case STREAMING_THRIFT:
            default:
                break;
        }
    }
}

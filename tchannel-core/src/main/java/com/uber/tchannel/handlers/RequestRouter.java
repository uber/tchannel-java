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

import com.uber.tchannel.api.Req;
import com.uber.tchannel.api.ReqHandler;
import com.uber.tchannel.api.Res;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.JSONSerializer;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.Serializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.HashMap;
import java.util.Map;

public class RequestRouter extends SimpleChannelInboundHandler<RawRequest> {

    private final Map<String, ? extends ReqHandler> requestHandlers;
    private final Serializer serializer;

    public RequestRouter(Map<String, ReqHandler> requestHandlers) {
        this.requestHandlers = requestHandlers;
        this.serializer = new Serializer(new HashMap<ArgScheme, Serializer.SerializerInterface>() {
            {
                put(ArgScheme.JSON, new JSONSerializer());
            }
        }
        );
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, RawRequest rawRequest) throws Exception {

        ArgScheme argScheme = ArgScheme.fromString(
                rawRequest.getTransportHeaders().get(TransportHeaders.ARG_SCHEME_KEY)
        );

        if (argScheme == null) {
            throw new RuntimeException("Missing `Arg Scheme` header");
        }

        // arg1
        String method = this.serializer.decodeEndpoint(rawRequest);

        // Get handler for this method
        ReqHandler<?, ?> handler = this.requestHandlers.get(method);

        if (handler == null) {
            throw new RuntimeException(String.format("No handler for %s", method));
        }

        // arg2
        Map<String, String> applicationHeaders = this.serializer.decodeHeaders(rawRequest);

        // arg3
        Object body = this.serializer.decodeBody(rawRequest, handler.getRequestType());

        // transform request into form the handler expects
        Req<?> request = new Req<>(
                method,
                applicationHeaders,
                body
        );

        // Handle the request
        Res<?> response = handler.handle((Req) request);

        RawResponse rawResponse = new RawResponse(
                rawRequest.getId(),
                rawRequest.getTransportHeaders(),
                this.serializer.encodeEndpoint(response.getEndpoint(), argScheme),
                this.serializer.encodeHeaders(response.getHeaders(), argScheme),
                this.serializer.encodeBody(response.getBody(), argScheme)
        );

        ctx.writeAndFlush(rawResponse);

    }
}

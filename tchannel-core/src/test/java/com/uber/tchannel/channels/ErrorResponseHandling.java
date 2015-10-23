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
package com.uber.tchannel.channels;

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.ErrorResponse;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.ResponseMessage;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ErrorResponseHandling {

    @Test
    public void testBadRequestErrorOnInvalidArgScheme() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", new EchoHandler());
        server.listen();

        int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        client.listen();

        RawRequest req = new RawRequest(
            1000,
            "server",
            null,
            "echo",
            "title",
            "hello"
        );

        req.getTransportHeaders().clear();
        req.getTransportHeaders().put(TransportHeaders.ARG_SCHEME_KEY, "hello");

        ListenableFuture<ResponseMessage> future = subClient.call(
            host,
            port,
            req
        );

        ErrorResponse res = (ErrorResponse)future.get(100, TimeUnit.MILLISECONDS);
        assertEquals(ErrorType.BadRequest, res.getErrorType());
        assertEquals("Expected incoming call to have \"as\" header set", res.getMessage());

        server.shutdown();
        client.shutdown();
    }

    @Test
    public void testRateLimiting()  throws Exception {
        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .setMaxQueuedRequests(0)
            .build();
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", new EchoHandler());
        server.listen();

        int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        SubChannel subClient = client.makeSubChannel("server");
        client.listen();

        RawRequest req = new RawRequest(
            1000,
            "server",
            null,
            "echo1",
            "title",
            "hello"
        );

        ListenableFuture<ResponseMessage> future = subClient.call(
            host,
            port,
            req
        );

        ErrorResponse res = (ErrorResponse)future.get(100, TimeUnit.MILLISECONDS);
        assertEquals(ErrorType.Busy, res.getErrorType());
        assertEquals("Service is busy", res.getMessage());

        server.shutdown();
        client.shutdown();
    }

    protected  class EchoHandler implements RequestHandler {
        @Override
        public RawResponse handle(RawRequest request) {
            RawResponse response = new RawResponse(
                request.getId(),
                ResponseCode.OK,
                request.getTransportHeaders(),
                request.getArg2(),
                request.getArg3()
            );

            return response;
        }
    }
}

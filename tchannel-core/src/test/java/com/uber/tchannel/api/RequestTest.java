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

package com.uber.tchannel.api;

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.handlers.JSONRequestHandler;
import io.netty.handler.logging.LogLevel;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RequestTest {

    @Test
    public void testGetBody() throws Exception {
        Request<String> request = new Request.Builder<>("Hello, World!", "some-serice", "some-endpoint").build();
        assertNotNull(request);
        assertEquals(String.class, request.getBody().getClass());
        assertEquals("Hello, World!", request.getBody());
    }

    @Test
    public void fullRequestResponse() throws Exception {

        final String requestBody = "Hello, World!";
        final int responseBody = 10;

        TChannel tchannel = new TChannel.Builder("tchannel-name")
                .register("endpoint", new JSONRequestHandler<String, Integer>() {
                    public Response<Integer> handleImpl(Request<String> request) {

                        assertEquals(requestBody, request.getBody());
                        return new Response.Builder<>(responseBody, request.getEndpoint(), ResponseCode.OK)
                                .setTransportHeaders(request.getTransportHeaders())
                                .build();
                    }
                })
                .setLogLevel(LogLevel.INFO)
                .build();

        tchannel.listen();

        Request<String> request = new Request.Builder<>(requestBody, "ping-service", "endpoint")
                .build();

        ListenableFuture<Response<Integer>> responsePromise = tchannel.callJSON(
                tchannel.getHost(),
                tchannel.getListeningPort(),
                request,
                Integer.class
        );

        Response<Integer> response = responsePromise.get(200000, TimeUnit.MILLISECONDS);

        assertEquals(responseBody, (int) response.getBody());

        tchannel.shutdown();

    }

    @Test
    public void handlerAsFunction() throws Exception {

        final String requestBody = "ping?";
        final String responseBody = "pong!";

        JSONRequestHandler<String, String> requestHandler = new JSONRequestHandler<String, String>() {

            public Response<String> handleImpl(Request<String> request) {
                assertEquals(requestBody, request.getBody());
                return new Response.Builder<>(responseBody, request.getEndpoint(), ResponseCode.OK).build();
            }
        };

        Request<String> request = new Request.Builder<>(requestBody, "some-service", "some-endpoint").build();

        Response<String> response = requestHandler.handleImpl(request);

        assertEquals(responseBody, response.getBody());
    }
}

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

import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.Promise;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RequestTest {

    @Test
    public void testGetBody() throws Exception {
        Request<String> request = new Request.Builder<>("Hello, World!")
                .setEndpoint("some-endpoint")
                .setService("some-service")
                .build();
        assertNotNull(request);
        assertEquals(String.class, request.getBody().getClass());
        assertEquals("Hello, World!", request.getBody());
    }

    @Test
    public void fullRequestResponse() throws Exception {

        final String requestBody = "Hello, World!";
        final int responseBody = 10;

        TChannel tchannel = new TChannel.Builder("tchannel-name")
                .register("endpoint", new RequestHandler<String, Integer>() {
                    @Override
                    public Response<Integer> handle(Request<String> request) {

                        assertEquals(requestBody, request.getBody());

                        return new Response.Builder<>(responseBody)
                                .setEndpoint(request.getEndpoint())
                                .setHeaders(request.getHeaders())
                                .build();

                    }

                    @Override
                    public Class<String> getRequestType() {
                        return String.class;
                    }

                    @Override
                    public Class<Integer> getResponseType() {
                        return Integer.class;
                    }
                })
                .setLogLevel(LogLevel.INFO)
                .build();

        tchannel.listen();

        Request<String> request = new Request.Builder<>(requestBody)
                .setEndpoint("endpoint")
                .setService("ping-service")
                .build();

        Promise<Response<Integer>> responsePromise = tchannel.callJSON(
                tchannel.getHost(),
                tchannel.getListeningPort(),
                request,
                Integer.class
        );

        Response<Integer> response = responsePromise.get(1000, TimeUnit.MILLISECONDS);

        assertEquals(responseBody, (int) response.getBody());

        tchannel.shutdown();

    }

    @Test
    public void handlerAsFunction() throws Exception {

        final String requestBody = "ping?";
        final String responseBody = "pong!";

        RequestHandler<String, String> requestHandler = new RequestHandler<String, String>() {
            @Override
            public Response<String> handle(Request<String> request) {
                assertEquals(requestBody, request.getBody());
                return new Response.Builder<>(responseBody)
                        .setEndpoint(request.getEndpoint())
                        .setHeaders(request.getHeaders())
                        .build();
            }

            @Override
            public Class<String> getRequestType() {
                return String.class;
            }

            @Override
            public Class<String> getResponseType() {
                return String.class;
            }
        };

        Request<String> request = new Request.Builder<>(requestBody)
                .setService("some-service")
                .setEndpoint("some-endpoint")
                .build();

        Response<String> response = requestHandler.handle(request);

        assertEquals(responseBody, response.getBody());
    }
}

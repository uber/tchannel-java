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
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.Response;
import io.netty.handler.logging.LogLevel;
import org.junit.Test;

import java.net.InetAddress;

import static java.lang.Thread.sleep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RequestTest {

    @Test
    public void testGetBody() throws Exception {
        RawRequest request = new RawRequest.Builder("some-serice", "some-endpoint")
            .setBody("Hello, World!")
            .build();
        assertNotNull(request);
        assertEquals(String.class, request.getBody().getClass());
        assertEquals("Hello, World!", request.getBody());
        request.release();
    }

    @Test
    public void fullRequestResponse() throws Exception {

        final String requestBody = "Hello, World!";
        final int responseBody = 10;

        TChannel tchannel = new TChannel.Builder("tchannel-name")
            .setServerHost(InetAddress.getByName("127.0.0.1"))
            .setLogLevel(LogLevel.INFO)
            .build();
        SubChannel subChannel = tchannel.makeSubChannel("tchannel-name")
            .register("endpoint", new JSONRequestHandler<String, Integer>() {
                public JsonResponse<Integer> handleImpl(JsonRequest<String> request) {
                    assertEquals(requestBody, request.getBody(String.class));
                    return new JsonResponse.Builder<Integer>(request)
                        .setTransportHeaders(request.getTransportHeaders())
                        .setBody(10)
                        .build();
            }
        });

        tchannel.listen();

        JsonRequest<String> request = new JsonRequest.Builder<String>("tchannel-name", "endpoint")
            .setTimeout(2000000)
            .setBody(requestBody)
            .build();

        ListenableFuture<JsonResponse<Integer>> responsePromise = subChannel.send(
            request,
            tchannel.getHost(),
            tchannel.getListeningPort()
        );

        JsonResponse<Integer> response = responsePromise.get();

        assertEquals(null, response.getError());
        assertEquals(responseBody, (int) response.getBody(Integer.class));
        response.release();

        tchannel.shutdown();
    }

    @Test
    public void notOkResponse() throws Exception {

        final String requestBody = "Hello, World!";

        TChannel tchannel = new TChannel.Builder("tchannel-name")
            .setServerHost(InetAddress.getByName("127.0.0.1"))
            .setLogLevel(LogLevel.INFO)
            .build();
        SubChannel subChannel = tchannel.makeSubChannel("tchannel-name")
            .register("endpoint", new JSONRequestHandler<String, Integer>() {
                public JsonResponse<Integer> handleImpl(JsonRequest<String> request) {
                    return new JsonResponse.Builder<Integer>(request)
                        .setTransportHeaders(request.getTransportHeaders())
                        .setResponseCode(ResponseCode.Error)
                        .build();
                }
            });

        tchannel.listen();

        JsonRequest<String> request = new JsonRequest.Builder<String>("tchannel-name", "endpoint")
            .setTimeout(2000000)
            .setBody(requestBody)
            .build();

        ListenableFuture<JsonResponse<Integer>> responsePromise = subChannel.send(
            request,
            tchannel.getHost(),
            tchannel.getListeningPort()
        );

        JsonResponse<Integer> response = responsePromise.get();

        assertEquals(null, response.getError());
        assertEquals(ResponseCode.Error, response.getResponseCode());
        response.release();

        tchannel.shutdown();
    }

    @Test
    public void requestTimeout() throws Exception {

        final String requestBody = "Hello, World!";
        final int responseBody = 10;

        TChannel tchannel = new TChannel.Builder("tchannel-name")
            .setServerHost(InetAddress.getByName("127.0.0.1"))
            .setLogLevel(LogLevel.INFO)
            .build();
        SubChannel subChannel = tchannel.makeSubChannel("tchannel-name")
            .register("endpoint", new JSONRequestHandler<String, Integer>() {
                public JsonResponse<Integer> handleImpl(JsonRequest<String> request) {
                    try {
                        sleep(150);
                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }

                    return new JsonResponse.Builder<Integer>(request)
                        .setTransportHeaders(request.getTransportHeaders())
                        .setBody(10)
                        .build();
                }
            });

        tchannel.listen();

        JsonRequest<String> request = new JsonRequest.Builder<String>("tchannel-name", "endpoint")
            .setTimeout(100)
            .setBody(requestBody)
            .build();

        ListenableFuture<JsonResponse<Integer>> responsePromise = subChannel.send(
            request,
            tchannel.getHost(),
            tchannel.getListeningPort()
        );

        JsonResponse<Integer> response = responsePromise.get();

        assertNotEquals(null, response.getError());
        assertEquals(1, response.getError().getId());
        assertEquals(ErrorType.Timeout, response.getError().getErrorType());
        assertTrue(response.getError().getMessage().startsWith("Request timeout after"));

        response.release();
        tchannel.shutdown();
    }

    @Test
    public void handlerAsFunction() throws Exception {

        final String requestBody = "ping?";
        final String responseBody = "pong!";

        JSONRequestHandler<String, String> requestHandler = new JSONRequestHandler<String, String>() {
            @Override
            public JsonResponse<String> handleImpl(JsonRequest<String> request) {
                assertEquals(requestBody, request.getBody(String.class));
                return new JsonResponse.Builder<String>(request)
                    .setBody(responseBody)
                    .build();
            }
        };

        JsonRequest<String> request =
            new JsonRequest.Builder<String>("some-service", "some-endpoint")
                .setBody(requestBody)
                .build();

        JsonResponse<String> response = requestHandler.handleImpl(request);
        assertEquals(responseBody, response.getBody(String.class));
        response.release();
    }
}

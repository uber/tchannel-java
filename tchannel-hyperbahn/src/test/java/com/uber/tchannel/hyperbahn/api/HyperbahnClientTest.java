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

package com.uber.tchannel.hyperbahn.api;

import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.JSONRequestHandler;
import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import io.netty.channel.ChannelFuture;

import org.junit.Test;

import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.hyperbahn.messages.AdvertiseRequest;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;

public class HyperbahnClientTest {

    @Test
    public void testAdvertise() throws Exception {
        AdvertiseResponseHandler responseHandler = new AdvertiseResponseHandler();
        TChannel server = createMockHyperbahn(responseHandler);

        TChannel tchannel = new TChannel.Builder("hyperbahn-service").build();

        List<InetSocketAddress> routers = new ArrayList<InetSocketAddress>() {{
            add(new InetSocketAddress("127.0.0.1", 8888));
        }};

        HyperbahnClient hyperbahnClient = new HyperbahnClient.Builder("service", tchannel)
                .setRouters(routers)
                .build();
        TFuture<JsonResponse<AdvertiseResponse>> responseFuture = hyperbahnClient.advertise();

        try (JsonResponse<AdvertiseResponse> response = responseFuture.get()) {
            assertNotNull(response);
            assertEquals(1, responseHandler.requestsReceived);
        }

        tchannel.shutdown();
        server.shutdown();
    }

    @Test
    public void testMultiAdvertise() throws Exception {
        AdvertiseResponseHandler responseHandler = new AdvertiseResponseHandler();
        TChannel server = createMockHyperbahn(responseHandler);

        TChannel tchannel = new TChannel.Builder("hyperbahn-service").build();

        List<InetSocketAddress> routers = new ArrayList<InetSocketAddress>() {{
            add(new InetSocketAddress("127.0.0.1", 8888));
        }};

        HyperbahnClient hyperbahnClient = new HyperbahnClient.Builder("service", tchannel)
            .setRouters(routers)
            .setAdvertiseInterval(100)
            .build();
        hyperbahnClient.advertise().addCallback(new TFutureCallback<JsonResponse<AdvertiseResponse>>() {
            @Override
            public void onResponse(JsonResponse<AdvertiseResponse> response) {
                response.release();
            }
        });

        sleep(500);
        assertTrue(responseHandler.requestsReceived >= 3);
        hyperbahnClient.shutdown();

        server.shutdown();
    }

    public static TChannel createMockHyperbahn(AdvertiseResponseHandler adHandler) throws Exception {
        final TChannel server = new TChannel.Builder("hyperbahn")
                .setServerHost(InetAddress.getByName(null))
                .setServerPort(8888)
                .build();

        server.makeSubChannel("hyperbahn").register("ad", adHandler);

        ChannelFuture f = server.listen();
        return server;
    }

    public class AdvertiseResponseHandler extends JSONRequestHandler<AdvertiseRequest, AdvertiseResponse> {
        public int requestsReceived = 0;

        @Override
        public JsonResponse<AdvertiseResponse> handleImpl(JsonRequest<AdvertiseRequest> request) {
            requestsReceived++;
            return new JsonResponse.Builder<AdvertiseResponse>(request)
                .setBody(new AdvertiseResponse(10))
                .build();
        }
    }
}

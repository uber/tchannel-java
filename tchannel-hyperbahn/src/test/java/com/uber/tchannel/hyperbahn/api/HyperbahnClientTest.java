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

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.handlers.JSONRequestHandler;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.hyperbahn.messages.AdvertiseRequest;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        ListenableFuture<Response<AdvertiseResponse>> responseFuture = hyperbahnClient.advertise();

        Response<AdvertiseResponse> response = responseFuture.get(1000, TimeUnit.MILLISECONDS);

        assertNotNull(response);
        assertEquals(responseHandler.requestReceived, true);

        tchannel.shutdown();
        server.shutdown();
    }

    public static TChannel createMockHyperbahn(AdvertiseResponseHandler adHandler) throws Exception {
        final TChannel server = new TChannel.Builder("autobahn")
                .register("ad", adHandler)
                .setServerHost(InetAddress.getByName("127.0.0.1"))
                .setServerPort(8888)
                .build();

        ChannelFuture f = server.listen();
        return server;
    }

    public class AdvertiseResponseHandler extends JSONRequestHandler<AdvertiseRequest, AdvertiseResponse> {
        public boolean requestReceived = false;

        @Override
        public Response<AdvertiseResponse> handleImpl(Request<AdvertiseRequest> request) {
            requestReceived = true;
            return new Response.Builder<>(new AdvertiseResponse(10), request.getEndpoint(), ResponseCode.OK).build();
        }
    }
}

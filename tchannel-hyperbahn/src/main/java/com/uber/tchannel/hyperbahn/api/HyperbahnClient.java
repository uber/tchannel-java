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

import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.hyperbahn.messages.AdvertiseRequest;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HyperbahnClient {

    private static final String HOSTS_FILE_PATH = "/etc/uber/hyperbahn/hosts.json";
    private static final String HYPERBAHN_SERVICE_NAME = "hyperbahn";
    private static final String HYPERBAHN_ADVERTISE_ENDPOINT = "ad";

    private final TChannel tchannel;
    private final List<InetSocketAddress> routers;
    private final Logger logger = LoggerFactory.getLogger(HyperbahnClient.class);

    public HyperbahnClient(TChannel tchannel) {
        this.tchannel = tchannel;
        this.routers = HyperbahnClient.loadRouters();
    }

    private static List<InetSocketAddress> loadRouters() {


    }

    public Response<AdvertiseResponse> advertise(
            String service,
            int cost
    ) throws InterruptedException, TimeoutException, ExecutionException {

        final AdvertiseRequest advertiseRequest = new AdvertiseRequest();
        advertiseRequest.addService(service, cost);

        final Request<AdvertiseRequest> request = new Request.Builder<>(
                advertiseRequest,
                HYPERBAHN_SERVICE_NAME,
                HYPERBAHN_ADVERTISE_ENDPOINT
        )
                .build();

        final InetSocketAddress router = this.

                Future < Response < AdvertiseResponse >> responseFuture = this.tchannel.callJSON(
                InetAddress.getLoopbackAddress(),
                21300,
                request,
                AdvertiseResponse.class
        );

        Response<AdvertiseResponse> response = responseFuture.get(1000, TimeUnit.MILLISECONDS);

        return response;

    }

}

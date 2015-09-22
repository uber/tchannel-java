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

import com.google.gson.Gson;
import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.hyperbahn.messages.AdvertiseRequest;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

public class HyperbahnClient {

    private static final String HOSTS_FILE_PATH = "/etc/uber/hyperbahn/hosts.json";
    private static final String HYPERBAHN_SERVICE_NAME = "hyperbahn";
    private static final String HYPERBAHN_ADVERTISE_ENDPOINT = "ad";

    private final TChannel tchannel;
    private final Logger logger = LoggerFactory.getLogger(HyperbahnClient.class);
    private List<InetSocketAddress> routers;

    public HyperbahnClient(TChannel tchannel) throws IOException {
        this.tchannel = tchannel;
        this.routers = HyperbahnClient.loadRouters();
    }

    private static List<InetSocketAddress> loadRouters() throws IOException {

        List<String> hostPorts;
        try (Reader reader = new FileReader(HyperbahnClient.HOSTS_FILE_PATH)) {
            hostPorts = new Gson().fromJson(reader, List.class);
        }

        List<InetSocketAddress> routers = new LinkedList<>();
        for (String hostPort : hostPorts) {
            String[] hostPortPair = hostPort.split(Pattern.quote(":"));
            String host = hostPortPair[0];
            int port = Integer.parseInt(hostPortPair[1]);
            InetSocketAddress router = new InetSocketAddress(host, port);
            routers.add(router);
        }

        return routers;

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

        final InetSocketAddress router = this.routers.get(new Random().nextInt(this.routers.size()));

        Future<Response<AdvertiseResponse>> responseFuture = this.tchannel.callJSON(
                router.getAddress(),
                router.getPort(),
                request,
                AdvertiseResponse.class
        );

        Response<AdvertiseResponse> response = responseFuture.get(1000, TimeUnit.MILLISECONDS);

        return response;

    }

    public void stopAdvertising() {

    }

    public void shutdown() throws InterruptedException {
        this.logger.info("Shutting down HyperbahnClient and TChannel.");
        this.stopAdvertising();
        this.tchannel.shutdown();
        this.routers.clear();
        this.logger.info("HyperbahnClient shutdown complete.");
    }

}

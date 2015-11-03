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

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.uber.tchannel.schemes.JsonRequest;
import com.uber.tchannel.schemes.JsonResponse;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.channels.Connection;
import com.uber.tchannel.hyperbahn.messages.AdvertiseRequest;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;
import com.uber.tchannel.schemes.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HyperbahnClient {
    private static final String HYPERBAHN_SERVICE_NAME = "hyperbahn";
    private static final String HYPERBAHN_ADVERTISE_ENDPOINT = "ad";
    public AtomicBoolean destroyed = new AtomicBoolean(false);
    private final Logger logger = LoggerFactory.getLogger(HyperbahnClient.class);

    private final String service;
    private final TChannel tchannel;
    private final SubChannel hyperbahnChannel;
    private final List<InetSocketAddress> routers;
    private final long advertiseTimeout;
    private final long advertiseInterval;

    private Timer advertiseTimer = new Timer(true);

    private HyperbahnClient(Builder builder) {
        this.service = builder.service;
        this.tchannel = builder.channel;
        this.routers = builder.routers;
        this.advertiseTimeout = builder.advertiseTimeout;
        this.advertiseInterval = builder.advertiseInterval;
        this.hyperbahnChannel = makeClientChannel(HYPERBAHN_SERVICE_NAME);
    }

    public SubChannel makeClientChannel(String service) {
        SubChannel subChannel = this.tchannel.makeSubChannel(service, Connection.Direction.IN)
            .setPeers(routers);

        return subChannel;
    }

    public ListenableFuture<JsonResponse<AdvertiseResponse>> advertise()
        throws InterruptedException, TChannelError {

        final AdvertiseRequest advertiseRequest = new AdvertiseRequest();
        advertiseRequest.addService(service, 0);

        // TODO: options for timeout, hard fail, etc.
        final JsonRequest<AdvertiseRequest> request = new JsonRequest.Builder<AdvertiseRequest>(
            HYPERBAHN_SERVICE_NAME,
            HYPERBAHN_ADVERTISE_ENDPOINT
        )
            .setBody(advertiseRequest)
            .setTTL(advertiseTimeout, TimeUnit.SECONDS)
            .build();

        Runnable runable = null;
        ListenableFuture<JsonResponse<AdvertiseResponse>> responseFuture = null;
        try {
            final ListenableFuture<JsonResponse<AdvertiseResponse>> future = responseFuture = hyperbahnChannel.send(request);
            runable = new Runnable() {
                @Override
                public void run() {
                    try {
                        JsonResponse<AdvertiseResponse> response = future.get();
                        // TODO: fix this hack
                        response.getHeaders();
                        response.getBody(AdvertiseResponse.class);
                        response.release();
                    } catch (Exception ex) {
                        logger.error("Advertise failure: " + ex.toString());
                    }

                    if (destroyed.get()) {
                        return;
                    }

                    scheduleAdvertise();
                }
            };
        } catch (TChannelError ex) {
            // TODO: should be moved into tchanne as retry ...
            this.logger.error("Advertise failure: " + ex.toString());

            // re-throw for now
            // TODO: should really handle internally
            throw ex;
        }

        responseFuture.addListener(runable, new Executor() {
            @Override
            public void execute(Runnable command) {
                command.run();
            }
        });

        return responseFuture;

    }

    private void scheduleAdvertise() {
        if (destroyed.get()) {
            return;
        }

        advertiseTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    ListenableFuture<JsonResponse<AdvertiseResponse>> responseFuture = advertise();
                } catch (Exception ex) {
                    logger.error(service + " failed to advertise. " + ex.getMessage());
                }
            }
        }, advertiseInterval);
    }

    public void stopAdvertising() {
        advertiseTimer.cancel();
    }

    public void shutdown() throws InterruptedException, ExecutionException {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }

        this.logger.info("Shutting down HyperbahnClient and TChannel.");
        this.stopAdvertising();
        this.tchannel.shutdown();
        this.routers.clear();
        this.logger.info("HyperbahnClient shutdown complete.");
    }

    public static class Builder {

        private final String service;
        private final TChannel channel;

        private List<InetSocketAddress> routers;
        private long advertiseTimeout = 5000;
        private long advertiseInterval = 60 * 1000;

        public Builder(String service, TChannel channel) {
            if (service == null) {
                throw new NullPointerException("`service` cannot be null");
            }

            if (channel == null) {
                throw new NullPointerException("`channel` cannot be null");
            }

            this.service = service;
            this.channel = channel;
        }

        public Builder setAdvertiseTimeout(long advertiseTimeout) {
            this.advertiseTimeout = advertiseTimeout;
            return this;
        }

        public Builder advertiseInterval(long advertiseInterval) {
            this.advertiseInterval = advertiseInterval;
            return this;
        }

        public Builder setRouters(List<InetSocketAddress> routers) {
            this.routers = routers;
            return this;
        }

        public Builder setRouterFile(String routerFile) throws IOException {
            this.routers = loadRouters(routerFile);
            return this;
        }

        private static List<InetSocketAddress> loadRouters(String hostsFilePath) throws IOException {

            List<String> hostPorts;
            try (Reader reader = new FileReader(hostsFilePath)) {
                hostPorts = new Gson().fromJson(reader, List.class);
            }

            List<InetSocketAddress> routers = new ArrayList<>();
            for (String hostPort : hostPorts) {
                String[] hostPortPair = hostPort.split(Pattern.quote(":"));
                String host = hostPortPair[0];
                int port = Integer.parseInt(hostPortPair[1]);
                InetSocketAddress router = new InetSocketAddress(host, port);
                routers.add(router);
            }

            return routers;

        }

        public HyperbahnClient build() {
            return new HyperbahnClient(this);
        }

    }

}

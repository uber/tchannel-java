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
import com.google.gson.reflect.TypeToken;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.channels.Connection;
import com.uber.tchannel.hyperbahn.messages.AdvertiseRequest;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import java.io.FileInputStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public final class HyperbahnClient {
    private static final Logger logger = LoggerFactory.getLogger(HyperbahnClient.class);

    private static final String HYPERBAHN_SERVICE_NAME = "hyperbahn";
    private static final String HYPERBAHN_ADVERTISE_ENDPOINT = "ad";
    public AtomicBoolean destroyed = new AtomicBoolean(false); // FIXME why is it public and non-final???

    private final String service;
    private final TChannel tchannel;
    private final SubChannel hyperbahnChannel;
    private final List<InetSocketAddress> routers;
    private final long advertiseTimeout;
    private final long advertiseInterval;

    private final Timer advertiseTimer = new Timer(true);

    private static final long REQUEST_TIMEOUT = 1000;

    private HyperbahnClient(Builder builder) {
        this.service = builder.service;
        this.tchannel = builder.channel;
        this.routers = builder.routers;
        this.advertiseTimeout = builder.advertiseTimeout;
        this.advertiseInterval = builder.advertiseInterval;
        this.hyperbahnChannel = makeClientChannel(HYPERBAHN_SERVICE_NAME);
    }

    public SubChannel makeClientChannel(String service) {
        return tchannel.makeSubChannel(service, Connection.Direction.IN)
            .setPeers(routers);
    }

    /**
     * Starts advertising on Hyperbahn at a fixed interval.
     *
     * @return a future that resolves to the response of the first advertise request
     */
    public TFuture<JsonResponse<AdvertiseResponse>> advertise() {

        final AdvertiseRequest advertiseRequest = new AdvertiseRequest();
        advertiseRequest.addService(service, 0);

        // TODO: options for hard fail, retries etc.
        final JsonRequest<AdvertiseRequest> request = new JsonRequest.Builder<AdvertiseRequest>(
            HYPERBAHN_SERVICE_NAME,
            HYPERBAHN_ADVERTISE_ENDPOINT
        )
            .setBody(advertiseRequest)
            .setTimeout(REQUEST_TIMEOUT)
            .setRetryLimit(4)
            .build();

        final TFuture<JsonResponse<AdvertiseResponse>> future = hyperbahnChannel.send(request);
        future.addCallback(new TFutureCallback<JsonResponse<AdvertiseResponse>>() {
            @Override
            public void onResponse(JsonResponse<AdvertiseResponse> response) {
                if (response.isError()) {
                    logger.error("Failed to advertise to Hyperbahn: {} - {}",
                        response.getError().getErrorType(),
                        response.getError().getMessage());
                }

                if (destroyed.get()) {
                    return;
                }

                scheduleAdvertise();
            }
        });

        return future;
    }

    private void scheduleAdvertise() {
        if (destroyed.get()) {
            return;
        }

        advertiseTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                advertise();
            }
        }, advertiseInterval);
    }

    public void stopAdvertising() {
        advertiseTimer.cancel();
    }

    public void shutdown() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }

        logger.info("Shutting down HyperbahnClient and TChannel.");
        this.stopAdvertising();
        this.tchannel.shutdown();
        this.routers.clear();
        logger.info("HyperbahnClient shutdown complete.");
    }

    private static final Type LIST_OF_STRINGS = new TypeToken<List<String>>(){}.getType();

    public static class Builder {

        private final @NotNull String service;
        private final @NotNull TChannel channel;

        private List<InetSocketAddress> routers;

        private long advertiseTimeout = 5000;
        private long advertiseInterval = 60 * 1000;

        public Builder(@NotNull String service, @NotNull TChannel channel) {
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

        public Builder setAdvertiseInterval(long advertiseInterval) {
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

        private static @NotNull List<InetSocketAddress> loadRouters(String hostsFilePath) throws IOException {

            List<String> hostPorts;
            try (Reader reader = new InputStreamReader(new FileInputStream(hostsFilePath), StandardCharsets.UTF_8)) {
                hostPorts = new Gson().fromJson(reader, LIST_OF_STRINGS);
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

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

import com.google.common.collect.ImmutableMap;
import com.uber.tchannel.api.errors.TChannelConnectionTimeout;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.api.errors.TChannelNoPeerAvailable;
import com.uber.tchannel.api.handlers.HealthCheckRequestHandler;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.channels.Connection;
import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.channels.SubPeer;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.handlers.OutRequest;
import com.uber.tchannel.handlers.ResponseRouter;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.messages.JSONSerializer;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.messages.Serializer;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.messages.ThriftSerializer;
import com.uber.tchannel.tracing.Tracing;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public final class SubChannel {

    private final String service;
    private final @NotNull TChannel topChannel;
    private final @NotNull PeerManager peerManager;
    private final long initTimeout;
    private final Connection.Direction preferredDirection;
    private final @NotNull List<SubPeer> peers = new ArrayList<>();
    private final @NotNull Map<String, RequestHandler> requestHandlers = new ConcurrentHashMap<>();

    private static final ImmutableMap<ArgScheme, Serializer.SerializerInterface> DEFAULT_SERIALIZERS =
            ImmutableMap.of(ArgScheme.JSON, new JSONSerializer(), ArgScheme.THRIFT, new ThriftSerializer());

    private final Serializer serializer = new Serializer(DEFAULT_SERIALIZERS);

    public SubChannel(String service, @NotNull TChannel topChannel) {
        this(service, topChannel, Connection.Direction.NONE);
    }

    public SubChannel(String service, @NotNull TChannel topChannel, Connection.Direction preferredDirection) {
        this.service = service;
        this.topChannel = topChannel;
        this.peerManager = topChannel.getPeerManager();
        this.initTimeout = topChannel.getInitTimeout();
        this.preferredDirection = preferredDirection;
    }

    public String getServiceName() {
        return this.service;
    }

    public @NotNull PeerManager getPeerManager() {
        return this.peerManager;
    }

    /**
     * Add a handler for a named endpoint. None of the parameters can be null.
     * @param endpoint name of endpoint
     * @param requestHandler request handler
     * @return same object (this)
     */
    public @NotNull SubChannel register(@NotNull String endpoint, @NotNull RequestHandler requestHandler) {
        requestHandlers.put(endpoint, requestHandler);
        return this;
    }

    /** @deprecated typo - use {@link #registerHealthHandler}. */
    @Deprecated
    public @NotNull SubChannel registerHealthHanlder() {
        return registerHealthHandler();
    }

    public @NotNull SubChannel registerHealthHandler() {
        return registerHealthHandler(new HealthCheckRequestHandler());
    }

    public @NotNull SubChannel registerHealthHandler(HealthCheckRequestHandler healthHandler) {
        return register("Meta::health", healthHandler);
    }

    /**
     * @param endpoint name of endpoint
     * @return Corresponding handler or null of no handler for endpoint
     */
    public @Nullable RequestHandler getRequestHandler(@Nullable String endpoint) {
        return requestHandlers.get(endpoint);
    }

    public Connection.Direction getPreferredDirection() {
        return preferredDirection;
    }

    public @NotNull SubChannel setPeers(@NotNull List<InetSocketAddress> peers) {
        for (InetSocketAddress peer : peers) {
            this.peers.add(new SubPeer(peer, this));
        }

        return this;
    }

    public @Nullable SubPeer choosePeer(@NotNull OutRequest<?> outRequest) {
        if (peers.isEmpty()) {
            return null;
        }

        int start = new Random().nextInt(peers.size());
        int i = start;
        boolean stop;
        SubPeer res = null;
        do {
            i = (i + 1) % peers.size();
            SubPeer peer = peers.get(i);
            stop = peer.updateScore(outRequest);
            if (stop || res == null || peer.getScore() > res.getScore()) {
                res = peer;
            }
        } while (!stop && i != start);

        outRequest.setUsedPeer(res.getRemoteAddress());
        return res;
    }

    public @Nullable Connection connect(@NotNull OutRequest<?> outRequest) {
        SubPeer peer = choosePeer(outRequest);
        if (peer == null) {
            return null;
        }

        Connection conn = peer.getPreferredConnection();
        if (conn == null) {
            conn = peer.connectTo();
        }

        return conn;
    }

    public boolean sendOutRequest(OutRequest<?> outRequest) {
        boolean res = false;
        while (true) {
            if (!outRequest.shouldRetry()) {
                outRequest.setFuture();
                break;
            }

            if (sendOutRequest(outRequest, connect(outRequest))) {
                res = true;
                break;
            }
        }

        return res;
    }

    public <T, U> TFuture<ThriftResponse<U>> send(
            ThriftRequest<T> request,
            InetAddress host,
            int port
    ) throws TChannelError {
        // Set the "cn" header
        // TODO: should make "cn" an option
        request.setTransportHeader(TransportHeaders.CALLER_NAME_KEY, this.topChannel.getServiceName());
        return sendRequest(request, host, port);
    }

    public <T, U> TFuture<ThriftResponse<U>> send(
        ThriftRequest<T> request
    ) throws TChannelError {
        return send(request, null, 0);
    }

    public <T, U> TFuture<JsonResponse<U>> send(
        JsonRequest<T> request,
        InetAddress host,
        int port
    ) {
        // Set the "cn" header
        // TODO: should make "cn" an option
        request.setTransportHeader(TransportHeaders.CALLER_NAME_KEY, this.topChannel.getServiceName());
        return sendRequest(request, host, port);
    }

    public <T, U> TFuture<JsonResponse<U>> send(
        JsonRequest<T> request
    ) {
        return send(request, null, 0);
    }

    public TFuture<RawResponse> send(
        RawRequest request,
        InetAddress host,
        int port
    ) {
        // Set the "cn" header
        // TODO: should make "cn" an option
        request.setTransportHeader(TransportHeaders.CALLER_NAME_KEY, this.topChannel.getServiceName());
        return sendRequest(request, host, port);
    }

    public TFuture<RawResponse> send(
        RawRequest request
    ) {
        return send(request, null, 0);
    }

    protected <V extends @NotNull Response> TFuture<V> sendRequest(
        Request request,
        InetAddress host,
        int port
    ) {
        OutRequest<V> outRequest = new OutRequest<>(this, request, topChannel.getTracingContext());
        if (host != null) {
            Connection conn = peerManager.findOrNew(new InetSocketAddress(host, port));
            // No retry for direct connections
            outRequest.disableRetry();
            if (!sendOutRequest(outRequest, conn)) {
                outRequest.setFuture();
            }
        } else if (peers.isEmpty()) {
            outRequest.setLastError(ErrorType.BadRequest, new TChannelNoPeerAvailable());
            outRequest.setFuture();
        } else {
            sendOutRequest(outRequest);
        }

        return outRequest.getFuture();
    }

    private boolean sendOutRequest(
        @NotNull OutRequest<?> outRequest,
        @Nullable Connection connection
    ) {
        Request request = outRequest.getRequest();

        try {
            // the (successfully created) tracing span is finish()'ed via callback on outRequest.getFuture()
            Tracing.startOutboundSpan(outRequest, topChannel.getTracer(), topChannel.getTracingContext());
        } catch (RuntimeException e) {
            outRequest.setLastError(ErrorType.BadRequest, e);
            outRequest.setFuture();
            return false;
        }

        // Validate if the ArgScheme is set correctly
        if (request.getArgScheme() == null) {
            request.setArgScheme(ArgScheme.RAW);
            outRequest.setLastError(ErrorType.BadRequest, "Expect call request to have Arg Scheme specified");
            outRequest.setFuture();
            return false;
        }

        // Set the default retry flag if it is not set
        if (request.getRetryFlags() == null) {
            request.setRetryFlags("c");
        }

        long initTimeout = this.initTimeout;
        if (initTimeout <= 0) {
            initTimeout = request.getTimeout();
        }

        if (connection == null) {
            outRequest.setLastError(ErrorType.BadRequest, new TChannelNoPeerAvailable());
            outRequest.setFuture();
            return false;
        } else if (!connection.waitForIdentified(initTimeout)) {
            connection.clean();
            if (connection.lastError() != null) {
                outRequest.setLastError(ErrorType.NetworkError, connection.lastError());
            } else {
                // TODO - extract the formatting and reuse all over
                String logMessage =  String.format("%s/%s::%s", connection.getPeer().remoteAddress,
                        request.getService(), request.getEndpoint());
                outRequest.setLastError(ErrorType.NetworkError, new TChannelConnectionTimeout(logMessage));
            }
            return false;
        }

        // Get a response router for our outbound channel
        ResponseRouter router = connection.channel().pipeline().get(ResponseRouter.class);
        return router.expectResponse(outRequest);
    }
}

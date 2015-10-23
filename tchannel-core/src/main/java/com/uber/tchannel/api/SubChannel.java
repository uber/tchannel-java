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

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.uber.tchannel.api.errors.TChannelConnectionTimeout;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.api.errors.TChannelNoPeerAvailable;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.channels.Connection;
import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.channels.SubPeer;
import com.uber.tchannel.handlers.ResponseRouter;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.ErrorResponse;
import com.uber.tchannel.schemes.JSONSerializer;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.ResponseMessage;
import com.uber.tchannel.schemes.Serializer;
import com.uber.tchannel.schemes.ThriftSerializer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.transform;

public final class SubChannel {

    private final String service;
    private final TChannel topChannel;
    private final PeerManager peerManager;
    private final long initTimeout;
    private final Connection.Direction preferredDirection;

    private Map<String, RequestHandler> requestHandlers = new HashMap<>();
    private List<SubPeer> peers = new ArrayList<>();

    private final Serializer serializer = new Serializer(new HashMap<ArgScheme, Serializer.SerializerInterface>() {
        {
            put(ArgScheme.JSON, new JSONSerializer());
            put(ArgScheme.THRIFT, new ThriftSerializer());
        }
    });

    public SubChannel(String service, TChannel topChannel) {
        this.service = service;
        this.topChannel = topChannel;
        this.peerManager = topChannel.getPeerManager();
        this.initTimeout = topChannel.getInitTimeout();
        this.preferredDirection = Connection.Direction.NONE;
    }

    public SubChannel(String service, TChannel topChannel, Connection.Direction preferredDirection) {
        this.service = service;
        this.topChannel = topChannel;
        this.peerManager = topChannel.getPeerManager();
        this.initTimeout = topChannel.getInitTimeout();
        this.preferredDirection = preferredDirection;
    }

    public String getServiceName() {
        return this.service;
    }

    public PeerManager getPeerManager() {
        return this.peerManager;
    }

    public SubChannel register(String endpoint, RequestHandler requestHandler) {
        requestHandlers.put(endpoint, requestHandler);
        return this;
    }

    public RequestHandler getRequestHandler(String endpoint) {
        return requestHandlers.get(endpoint);
    }

    public Connection.Direction getPreferredDirection() {
        return preferredDirection;
    }

    public SubChannel setPeers(List<InetSocketAddress> peers) {
        for (InetSocketAddress peer : peers) {
            this.peers.add(new SubPeer(peer, this));
        }

        return this;
    }

    public SubPeer choosePeer() {
        SubPeer res = null;
        for (SubPeer peer : peers) {
            peer.updateScore();
            if (res == null) {
                res = peer;
            } else if (peer.getScore() > res.getScore()) {
                res = peer;
            }
        }

        return res;
    }

    public Connection connect() throws TChannelError {
        SubPeer peer = choosePeer();
        Connection conn = peer.getPreferredConnection();
        if (conn == null) {
            conn = peer.connectTo();
        }

        return conn;
    }

    private <T, U> ListenableFuture<Response<T>> callWithEncoding(
            InetAddress host,
            int port,
            Request<U> request,
            final Class<T> responseType,
            ArgScheme scheme
    ) throws InterruptedException, TChannelError {

        RawRequest rawRequest = new RawRequest(
                request.getTTL(),
                request.getService(),
                request.getTransportHeaders(),
                serializer.encodeEndpoint(request.getEndpoint(), scheme),
                serializer.encodeHeaders(request.getHeaders(), scheme),
                serializer.encodeBody(request.getBody(), scheme)
        );

        // Set the "cn" header
        // TODO: should make "cn" an option
        rawRequest.setTransportHeader(TransportHeaders.CALLER_NAME_KEY, this.topChannel.getServiceName());
        rawRequest.setTransportHeader(TransportHeaders.ARG_SCHEME_KEY, scheme.getScheme());

        ListenableFuture<ResponseMessage> future = this.call(host, port, rawRequest);
        return transform(future, new AsyncFunction<ResponseMessage, Response<T>>() {
            @Override
            public ListenableFuture<Response<T>> apply(ResponseMessage responseMessage) {
                SettableFuture<Response<T>> settableFuture = SettableFuture.create();
                Response<T> response = null;
                if (responseMessage instanceof ErrorResponse) {
                    response = new Response.Builder<T>((ErrorResponse) responseMessage).build();
                } else {
                    response = new Response.Builder<>(
                        serializer.decodeBody((RawResponse) responseMessage, responseType),
                        ((RawResponse) responseMessage).getResponseCode())
                        .setHeaders(serializer.decodeHeaders((RawResponse) responseMessage))
                        .build();
                }

                settableFuture.set(response);
                return settableFuture;
            }
        });
    }

    public <T, U> ListenableFuture<Response<T>> callThrift(
            InetAddress host,
            int port,
            Request<U> request,
            final Class<T> responseType
    ) throws InterruptedException, TChannelError {
        return callWithEncoding(host, port, request, responseType, ArgScheme.THRIFT);
    }

    public <T, U> ListenableFuture<Response<T>> callThrift(
        Request<U> request,
        final Class<T> responseType
    ) throws InterruptedException, TChannelError {
        return callWithEncoding(null, 0, request, responseType, ArgScheme.THRIFT);
    }

    public <T, U> ListenableFuture<Response<T>> callJSON(
            InetAddress host,
            int port,
            Request<U> request,
            final Class<T> responseType
    ) throws InterruptedException, TChannelError {
        return callWithEncoding(host, port, request, responseType, ArgScheme.JSON);
    }

    public <T, U> ListenableFuture<Response<T>> callJSON(
        Request<U> request,
        final Class<T> responseType
    ) throws InterruptedException, TChannelError {
        return callWithEncoding(null, 0, request, responseType, ArgScheme.JSON);
    }

    public ListenableFuture<ResponseMessage> call(
            InetAddress host,
            int port,
            RawRequest request
    ) throws InterruptedException, TChannelError {

        // Get an outbound channel
        Connection conn = null;

        if (host != null) {
            conn = this.peerManager.findOrNew(new InetSocketAddress(host, port));
        } else if (peers.size() == 0) {
            throw new TChannelNoPeerAvailable();
        } else {
            conn = connect();
        }

        if (!conn.waitForIdentified(this.initTimeout)) {
            if (conn.lastError() != null) {
                throw conn.lastError();
            } else {
                throw new TChannelConnectionTimeout();
            }
        }

        // Set the ArgScheme as RAW if its not set
        Map<String, String> transportHeaders = request.getTransportHeaders();
        if (!transportHeaders.containsKey(TransportHeaders.ARG_SCHEME_KEY)) {
            request.setTransportHeader(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
        }

        // Get a response router for our outbound channel
        ResponseRouter responseRouter = conn.channel().pipeline().get(ResponseRouter.class);

        // Ask the router to make a call on our behalf, and return its promise
        return responseRouter.expectResponse(request);
    }

    public ListenableFuture<ResponseMessage> call(
        RawRequest request
    ) throws InterruptedException, TChannelError {
        return call(null, 0, request);
    }
}

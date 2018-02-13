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

package com.uber.tchannel.channels;

import com.google.common.collect.Maps;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PeerManager manages peers, a abstract presentation of a channel to a host_port.
 */
public class PeerManager {

    private static final Logger logger = LoggerFactory.getLogger(PeerManager.class);

    private final Bootstrap clientBootstrap;
    private final @NotNull ConcurrentHashMap<SocketAddress, Peer> peers = new ConcurrentHashMap<>();

    // mapping from channel to actual remote address when client is not ephemeral
    private final ConcurrentHashMap<ChannelId, SocketAddress> channelTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChannelId, Connection> inConnections = new ConcurrentHashMap<>();
    private String hostPort = "0.0.0.0:0";

    public PeerManager(Bootstrap clientBootstrap) {
        this.clientBootstrap = clientBootstrap;
    }

    public Connection findOrNew(@NotNull SocketAddress address) {
        return findOrNewPeer(address).connect(this.clientBootstrap);
    }

    public Peer findOrNewPeer(@NotNull SocketAddress address) {
        Peer peer = peers.get(address);
        if (peer == null) {
            peers.putIfAbsent(address, new Peer(this, address));
            peer = peers.get(address);
        }
        return peer;
    }

    public @Nullable Peer getPeer(@NotNull SocketAddress address) {
        return peers.get(address);
    }

    public @Nullable Peer getPeer(@NotNull Channel channel) {
        return peers.get(channel.remoteAddress());
    }

    public Connection connectTo(@NotNull SocketAddress address) {
        return findOrNewPeer(address).connect(this.clientBootstrap, Connection.Direction.OUT);
    }

    public @Nullable Connection get(@NotNull Channel channel) {
        Peer peer = peers.get(channel.remoteAddress());
        return peer == null ? null : peer.getConnection(channel.id());
    }

    public void add(@NotNull ChannelHandlerContext ctx) {
        // Direction only matters for the init path when the init handler hasn't been removed
        if (!ctx.pipeline().names().contains("InitRequestHandler")) {
            findOrNewPeer(ctx.channel().remoteAddress()).handleActiveOutConnection(ctx);
        }
    }

    public @Nullable Connection remove(@NotNull Channel channel) {
        Peer peer = peers.get(channel.remoteAddress());
        if (peer != null) {
            return peer.remove(channel);
        }

        SocketAddress address = channelTable.remove(channel.id());
        if (address != null) {
            peer = peers.get(address);
            if (peer != null) {
                return peer.remove(channel);
            }
        }

        // TODO: clean up when connection count drops to 0
        return null;
    }

    public void setIdentified(@NotNull Channel channel, @NotNull Map<String, String> headers) {
        Connection conn = get(channel);
        if (conn == null) {
            // Handle in connection
            conn = new Connection(null, channel, Connection.Direction.IN);
        }

        conn.setIdentified(headers);
        if (!conn.isEphemeral() && conn.direction == Connection.Direction.IN) {
            SocketAddress address = conn.getRemoteAddressAsSocketAddress();
            channelTable.put(channel.id(), address);
            Peer peer = findOrNewPeer(address);
            conn.setPeer(peer);
            peer.add(conn);
        }
    }

    public void handleConnectionErrors(@NotNull Channel channel, @NotNull Throwable cause) {
        logger.error("Resetting connection due to the error.", cause);
        Connection conn = remove(channel);
        if (conn != null) {
            conn.clean();
        }
    }

    public void close() {
        for (Peer peer : peers.values()) {
            peer.close();
        }

        peers.clear();
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public String getHostPort() {
        return hostPort;
    }

    // TODO: peer stats & reaper
    public @NotNull Map<String, Integer> getStats() {
        int in = 0;
        int out = 0;
        for (Peer peer : peers.values()) {
            Map<String, Integer> connStats = peer.getStats();
            in += connStats.get("connections.in");
            out += connStats.get("connections.out");
        }

        Map<String, Integer> result = Maps.newHashMapWithExpectedSize(2);
        result.put("connections.in", in);
        result.put("connections.out", out);
        return result;
    }

}

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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PeerManager manages peers, a abstract presentation of a channel to a host_port.
 */
public class PeerManager {
    private static final Logger logger = LoggerFactory.getLogger(PeerManager.class);

    private final Bootstrap clientBootstrap;
    private final ConcurrentHashMap<SocketAddress, Peer> peers = new ConcurrentHashMap<>();

    // mapping from channel to actual remote address when client is not ephemeral
    private final ConcurrentHashMap<ChannelId, SocketAddress> channelTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChannelId, Connection> inConnections = new ConcurrentHashMap<>();
    private String hostPort = "0.0.0.0:0";

    public PeerManager(Bootstrap clientBootstrap) {
        this.clientBootstrap = clientBootstrap;
    }

    public Connection findOrNew(SocketAddress address) {
        Peer peer = peers.get(address);
        if (peer == null) {
            peer = new Peer(this, address);
            peers.putIfAbsent(address, peer);
            peer = peers.get(address);
        }

        return peer.connect(this.clientBootstrap);
    }

    public Peer findOrNewPeer(SocketAddress address) {
        Peer peer = peers.get(address);
        if (peer == null) {
            peer = new Peer(this, address);
            peers.putIfAbsent(address, peer);
            peer = peers.get(address);
        }

        return peer;
    }

    public Peer getPeer(SocketAddress address) {
        return peers.get(address);
    }

    public Peer getPeer(Channel channel) {
        SocketAddress address = channel.remoteAddress();
        return peers.get(address);
    }

    public Connection connectTo(SocketAddress address) {
        Peer peer = findOrNewPeer(address);
        return peer.connect(this.clientBootstrap, Connection.Direction.OUT);
    }

    public Connection get(Channel channel) {
        SocketAddress address = channel.remoteAddress();
        Peer peer = peers.get(address);
        if (peer != null) {
            return peer.getConnection(channel.id());
        }

        return null;
    }

    public void add(ChannelHandlerContext ctx) {
        // Direction only matters for the init path when the
        // init handler hasn't been removed
        Connection.Direction direction = Connection.Direction.OUT;
        if (ctx.pipeline().names().contains("InitRequestHandler")) {
            direction = Connection.Direction.IN;
        }

        if (direction == Connection.Direction.IN) {
            return;
        }

        Channel channel = ctx.channel();
        SocketAddress address = channel.remoteAddress();
        Peer peer = findOrNewPeer(address);
        peer.handleActiveOutConnection(ctx);
    }

    public Connection remove(Channel channel) {
        SocketAddress address = channel.remoteAddress();
        Peer peer = peers.get(address);
        if (peer != null) {
            return peer.remove(channel);
        }

        address = channelTable.remove(channel.id());
        if (address == null) {
            return null;
        }

        peer = peers.get(address);
        if (peer != null) {
            return peer.remove(channel);
        }

        // TODO: clean up when conneciton count drops to 0
        return null;
    }

    public void setIdentified(Channel channel, Map<String, String> headers) {
        Connection conn = get(channel);
        if (conn == null) {
            // Handle in connection
            conn = new Connection(null, channel, Connection.Direction.IN);
        }

        conn.setIndentified(headers);
        if (!conn.isEphemeral() && conn.direction == Connection.Direction.IN) {
            SocketAddress address = conn.getRemoteAddressAsSocketAddress();
            channelTable.put(channel.id(), address);
            Peer peer = findOrNewPeer(address);
            conn.setPeer(peer);
            peer.add(conn);
        }
    }

    public void handleConnectionErrors(Channel channel, Throwable cause) {
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
    public Map<String, Integer> getStats() {
        int in = 0;
        int out = 0;
        for (Peer peer : peers.values()) {
            Map<String, Integer> connStats = peer.getStats();
            in += connStats.get("connections.in");
            out += connStats.get("connections.out");
        }

        Map<String, Integer> result = new HashMap<>(3);
        result.put("connections.in", in);
        result.put("connections.out", out);
        return result;
    }
}

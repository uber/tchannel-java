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

import java.net.SocketAddress;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

/**
 * ChannelManager keeps track of open channels, and provides a way to lookup Channels by their Remote Address.
 * <p>
 * ChannelManager} maintains both a ChannelGroup of open connections and a Map of InetSocketAddress to ChannelId so that
 * connections can be reused if a request is going to the same remote. The ChannelManager also provides a convenient
 * entry point for shutting down all active Channels.
 */
public class ChannelManager {
    private final Map<SocketAddress, Peer> peers = new Hashtable<>();
    private String host = "0.0.0.0";
    private int port = 0;

    public Connection findOrNew(SocketAddress address, Bootstrap bootstrap) throws InterruptedException {
        Peer peer = peers.get(address);
        if (peer == null) {
            synchronized (peers) {
                peer = new Peer(this, address);
                peer.connect(bootstrap);
                peers.put(address, peer);
            }
        }

        Connection conn = peer.getConnection(ConnectionState.IDENTIFIED);
        return conn;
    }

    public Connection get(Channel channel) {
        SocketAddress address = channel.remoteAddress();
        Peer peer = peers.get(address);
        if (peer != null) {
            return peer.getConnection(channel.id());
        }

        return null;
    }

    public Channel add(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        System.out.println("Connection is active to " + channel.remoteAddress().toString());

        SocketAddress address = channel.remoteAddress();
        Peer peer = peers.get(address);
        if (peer == null) {
            synchronized (peers) {
                peer = new Peer(this, address);
                peers.put(address, peer);
            }
        }

        return peer.handleActiveConnection(ctx, getDirection(ctx)).channel;
    }

    public static Connection.Direction getDirection(ChannelHandlerContext ctx) {
        if (ctx.pipeline().names().contains("InitRequestHandler")) {
            return Connection.Direction.IN;
        } else {
            return Connection.Direction.OUT;
        }
    }

    public void remove(Channel channel) throws InterruptedException {
        SocketAddress address = channel.remoteAddress();
        Peer peer = peers.get(address);
        if (peer != null) {
            peer.remove(channel);
        }

        // TODO: clean up when conneciton count drops to 0
    }

    public void setIdentified(Channel channel, Map<String, String> headers) {
        Connection conn = get(channel);
        if (conn != null) {
            conn.setIndentified(headers);
        }
    }

    public boolean waitForIdentified(Channel channel, long timeout) {
        Connection conn = get(channel);
        if (conn != null) {
            return conn.waitForIdentified(timeout);
        }

        return false;
    }

    public void close() throws InterruptedException {
        synchronized (peers) {
            Iterator it = peers.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                ((Peer) pair.getValue()).close();
            }

            peers.clear();
        }
    }

    public synchronized void setHostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public synchronized String getHostPort() {
        return String.format("%s:%d", this.host, this.port);
    }
}

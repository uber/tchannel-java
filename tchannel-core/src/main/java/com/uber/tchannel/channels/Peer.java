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

import java.util.ArrayList;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Hashtable;
import java.util.HashMap;

import com.uber.tchannel.messages.InitMessage;
import com.uber.tchannel.messages.InitRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

/**
 * Peer manages connections to/from the same host_port. It provides a way to choose connections based on
 * their current status, e.g., connected, identified, etc.
 */
public class Peer {
    public ArrayList<Connection> connections = new ArrayList<>();
    public Map<ChannelId, Connection> maps = new Hashtable<>();
    public SocketAddress remoteAddress = null;

    private PeerManager manager;

    public Peer(PeerManager manager, SocketAddress remoteAddress) {
        this.manager = manager;
        this.remoteAddress = remoteAddress;
    }

    public synchronized Connection add(Connection connection) {
        if (!maps.containsKey(connection.channel().id())) {
            maps.put(connection.channel().id(), connection);
            connections.add(connection);
        }

        return connection;
    }

    public synchronized Connection add(Channel channel, Connection.Direction direction) {
        Connection conn = maps.get(channel.id());
        if (conn == null) {
            conn = new Connection(channel, direction);
            add(conn);
        }

        return conn;
    }

    public synchronized Connection handleActiveConnection(ChannelHandlerContext ctx, Connection.Direction direction) {
        Channel channel = ctx.channel();
        Connection conn;
        conn = add(channel, direction);

        if (conn.direction == Connection.Direction.OUT) {
            // Sending out the init request
            InitRequest initRequest = new InitRequest(0,
                InitMessage.DEFAULT_VERSION,
                new HashMap<String, String>() { }
            );
            initRequest.setHostPort(this.manager.getHostPort());
            // TODO: figure out what to put here
            initRequest.setProcessName("java-process");

            ChannelFuture f = ctx.writeAndFlush(initRequest);
            f.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }

        return conn;
    }

    public synchronized void remove(Connection connection) {
        connections.remove(connection);
        maps.remove(connection.channel().id());
    }

    public synchronized Connection remove(Channel channel) {
        Connection conn = maps.get(channel.id());
        if (conn != null) {
            maps.remove(channel.id());
            connections.remove(connections);
        }

        return conn;
    }

    public synchronized Connection connect(Bootstrap bootstrap) throws InterruptedException {
        Connection conn = getConnection(ConnectionState.IDENTIFIED);
        if (conn != null) {
            return conn;
        }

        Channel channel = bootstrap.connect(remoteAddress).sync().channel();
        return add(channel, Connection.Direction.OUT);
    }

    public synchronized Connection getConnection(ConnectionState preferedState) {
        Connection conn = null;
        for (int i = 0; i < connections.size(); i++) {
            conn = connections.get(i);
            if (conn.satisfy(preferedState)) {
                break;
            }
        }

        return conn;
    }

    public synchronized Connection getConnection(ChannelId channelId) {
        return maps.get(channelId);
    }

    public synchronized void close() throws InterruptedException {
        for (int i = 0; i < connections.size(); i++) {
            connections.get(i).close();
        }

        this.connections.clear();
        this.maps.clear();
    }
}

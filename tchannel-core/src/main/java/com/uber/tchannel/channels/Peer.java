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
import com.uber.tchannel.api.errors.TChannelConnectionFailure;
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.frames.InitFrame;
import com.uber.tchannel.frames.InitRequestFrame;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Peer manages connections to/from the same host_port. It provides a way to choose connections based on
 * their current status, e.g., connected, identified, etc.
 */
public class Peer {

    // FIXME why are these not private?
    public final @NotNull ConcurrentHashMap<ChannelId, Connection> connections = new ConcurrentHashMap<>();

    public final SocketAddress remoteAddress;

    private final @NotNull PeerManager manager;

    public Peer(@NotNull PeerManager manager, SocketAddress remoteAddress) {
        this.manager = manager;
        this.remoteAddress = remoteAddress;
    }

    public @NotNull Connection add(@NotNull Connection connection) {
        Connection conn = connections.putIfAbsent(connection.channel().id(), connection);
        return conn == null ? connection : conn;
    }

    public @NotNull Connection add(@NotNull Channel channel, Connection.Direction direction) {
        Connection conn = connections.get(channel.id());
        return conn == null ? add(new Connection(this, channel, direction)) : conn;
    }

    public @NotNull Connection handleActiveOutConnection(@NotNull ChannelHandlerContext ctx) {
        Connection conn = add(ctx.channel(), Connection.Direction.OUT);
        // Sending out the init request
        InitRequestFrame initRequestFrame = new InitRequestFrame(
            0,
            InitFrame.DEFAULT_VERSION,
            new HashMap<String, String>()
        );
        initRequestFrame.setHostPort(this.manager.getHostPort());
        // TODO: figure out what to put here
        initRequestFrame.setProcessName("java-process");
        MessageCodec.write(ctx, initRequestFrame);
        return conn;
    }

    public void remove(@NotNull Connection connection) {
        connections.remove(connection.channel().id());
    }

    public @Nullable Connection remove(@NotNull Channel channel) {
        return connections.remove(channel.id());
    }

    public @NotNull Connection connect(Bootstrap bootstrap, Connection.Direction preferredDirection) {
        Connection conn = getConnection(ConnectionState.IDENTIFIED, preferredDirection);
        if (conn != null && (preferredDirection == Connection.Direction.IN || conn.satisfy(preferredDirection))) {
            return conn;
        }

        final ChannelFuture f = bootstrap.connect(remoteAddress);
        Channel channel = f.channel();
        final Connection connection = add(channel, Connection.Direction.OUT);

        // handle connection errors
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    connection.setIdentified(new TChannelConnectionFailure(future.cause()));
                }
            }
        });

        return connection;
    }

    public @NotNull Connection connect(Bootstrap bootstrap) {
        return connect(bootstrap, Connection.Direction.NONE);
    }

    public @Nullable Connection getConnection(
        @Nullable ConnectionState preferredState, Connection.Direction preferredDirection
    ) {
        Connection conn = null;
        for (Connection next : connections.values()) {
            if (next.satisfy(preferredState)) {
                conn = next;
                if (preferredDirection == Connection.Direction.NONE || conn.direction == preferredDirection) {
                    break;
                }
            } else if (conn == null) {
                conn = next;
            }
        }
        return conn;
    }

    public @Nullable Connection getConnection(ConnectionState preferredState) {
        return getConnection(preferredState, Connection.Direction.NONE);
    }

    public @Nullable Connection getConnection(@NotNull ChannelId channelId) {
        return connections.get(channelId);
    }

    public void close() {
        for (Connection conn : connections.values()) {
            conn.close();
        }
        this.connections.clear();

    }

    public @NotNull Map<String, Integer> getStats() {
        int in = 0;
        int out = 0;
        for (Connection conn : connections.values()) {
            if (conn.direction == Connection.Direction.OUT) {
                out++;
            } else {
                in++;
            }
        }

        Map<String, Integer> result = Maps.newHashMapWithExpectedSize(2);
        result.put("connections.in", in);
        result.put("connections.out", out);
        return result;
    }

}

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
import io.netty.channel.ChannelId;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * ChannelManager keeps track of open channels, and provides a way to lookup Channels by their Remote Address.
 * <p>
 * ChannelManager} maintains both a ChannelGroup of open connections and a Map of InetSocketAddress to ChannelId so that
 * connections can be reused if a request is going to the same remote. The ChannelManager also provides a convenient
 * entry point for shutting down all active Channels.
 */
public class ChannelManager {

    private final Map<InetSocketAddress, ChannelId> channelMap = new HashMap<>();
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public boolean add(Channel channel) {
        this.channelMap.put((InetSocketAddress) channel.remoteAddress(), channel.id());
        return this.channels.add(channel);
    }

    public boolean remove(Channel channel) {
        this.channelMap.remove(channel.remoteAddress());
        return this.channels.remove(channel);
    }

    public Channel findOrNew(InetSocketAddress address, Bootstrap bootstrap) throws InterruptedException {
        Channel channel = this.find(address);

        if (channel == null) {
            channel = this.newChannel(address, bootstrap);
        }

        return channel;
    }

    public void close() throws InterruptedException {
        this.channels.close().sync();
        this.channels.clear();
        this.channelMap.clear();
    }

    public Channel find(InetSocketAddress address) {
        ChannelId channelId = this.channelMap.get(address);
        if (channelId == null) {
            return null;
        }
        return this.channels.find(channelId);
    }

    public Channel newChannel(InetSocketAddress address, Bootstrap bootstrap) throws InterruptedException {
        Channel channel = bootstrap.connect(address).sync().channel();
        this.add(channel);
        return channel;
    }
}

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

public class ChannelManager {

    private final Map<InetSocketAddress, ChannelId> channelMap = new HashMap<>();
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public boolean add(Channel channel) {
        this.channelMap.put((InetSocketAddress) channel.remoteAddress(), channel.id());
        return this.channels.add(channel);
    }

    public boolean remove(Channel ch) {
        this.channelMap.remove((InetSocketAddress) ch.remoteAddress());
        return this.channels.remove(ch);
    }

    public Channel findOrNew(InetSocketAddress address, Bootstrap bootstrap) throws InterruptedException {
        Channel ch = this.find(address);

        if (ch == null) {
            ch = this.newChannel(address, bootstrap);
        }

        return ch;
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
        Channel ch = bootstrap.connect(address).sync().channel();
        this.add(ch);
        return ch;
    }
}

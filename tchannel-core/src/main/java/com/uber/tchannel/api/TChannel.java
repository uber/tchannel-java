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

import com.uber.tchannel.schemes.RawResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class TChannel {

    private final String service;
    private final ServerBootstrap serverBootstrap;
    private int port;
    private Channel channel;

    public TChannel(String service, int port, ServerBootstrap serverBootstrap) {
        this.service = service;
        this.serverBootstrap = serverBootstrap;
    }

    public ChannelFuture start() throws InterruptedException {
        ChannelFuture f = this.serverBootstrap.bind(port).sync();
        this.channel = f.channel();
        return f;
    }

    public void stop() throws InterruptedException {
        this.channel.closeFuture().sync();
    }

    public FutureTask<Response> request(final Request request) {
        return new FutureTask<Response>(new Callable<Response>() {
            public Response call() throws Exception {
                System.out.println(request);
                return new RawResponse(
                        42,
                        new HashMap<String, String>(),
                        Unpooled.wrappedBuffer(Unpooled.EMPTY_BUFFER),
                        Unpooled.wrappedBuffer("headers".getBytes()),
                        Unpooled.wrappedBuffer("payload".getBytes())
                );
            }
        });
    }

}

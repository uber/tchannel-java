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

import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.codecs.TFrameCodec;
import com.uber.tchannel.framing.TFrame;
import com.uber.tchannel.handlers.InitRequestHandler;
import com.uber.tchannel.handlers.MessageMultiplexer;
import com.uber.tchannel.handlers.RequestHandlerHarness;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class TChannel {

    public final String channelName;
    private Map<String, RequestHandler> requestHandlers = new HashMap<String, RequestHandler>();

    public TChannel(String channelName) {
        this.channelName = channelName;
    }

    private ChannelInitializer<ServerChannel> channelInitializer(final Map<String, RequestHandler> requestHandlers) {
        return new ChannelInitializer<ServerChannel>() {
            @Override
            protected void initChannel(ServerChannel ch) throws Exception {
                // Translates TCP Streams to Raw Frames
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(TFrame.MAX_FRAME_LENGTH, 0, 2, -2, 0, true));

                // Translates Raw Frames into TFrames
                ch.pipeline().addLast(new TFrameCodec());

                // Translates TFrames into Messages
                ch.pipeline().addLast(new MessageCodec());

                // Handles Protocol Handshake
                ch.pipeline().addLast(new InitRequestHandler());

                // Handles Call Request RPC
                ch.pipeline().addLast(new MessageMultiplexer());

                for (Map.Entry<String, RequestHandler> entry : requestHandlers.entrySet()) {
                    ch.pipeline().addLast(
                            new DefaultEventLoopGroup(),
                            new RequestHandlerHarness(entry.getKey(), entry.getValue())
                    );
                }

            }
        };
    }

    private ServerBootstrap serverBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(new NioEventLoopGroup(), new NioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(this.channelInitializer(this.requestHandlers))
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .validate();
        return bootstrap;
    }

    public Channel start(int port) throws InterruptedException {
        ChannelFuture f = this.serverBootstrap().bind(port);
        f.await();
        return f.channel();
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

    public TChannel register(String endpoint, RequestHandler requestHandler) {
        this.requestHandlers.put(endpoint, requestHandler);
        return this;
    }

}


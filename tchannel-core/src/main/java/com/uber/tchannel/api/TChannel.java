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
import com.uber.tchannel.codecs.TChannelLengthFieldBasedFrameDecoder;
import com.uber.tchannel.codecs.TFrameCodec;
import com.uber.tchannel.handlers.InitRequestHandler;
import com.uber.tchannel.handlers.MessageMultiplexer;
import com.uber.tchannel.handlers.RequestDispatcher;
import com.uber.tchannel.schemes.RawResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.HashMap;
import java.util.Map;
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

    public static class TChannelServerBuilder {

        private final String service;
        private int port;
        private Map<String, RequestHandler> requestHandlers = new HashMap<String, RequestHandler>();
        private EventLoopGroup bossGroup;
        private EventLoopGroup childGroup;
        private EventLoopGroup workerGroup;
        private LogLevel logLevel;

        public TChannelServerBuilder(String service) {
            if (service == null) {
                throw new NullPointerException("`service` cannot be null");
            }
            this.service = service;
        }

        public TChannelServerBuilder setPort(int port) {
            this.port = port;
            return this;
        }

        public TChannelServerBuilder register(String service, RequestHandler requestHandler) {
            this.requestHandlers.put(service, requestHandler);
            return this;
        }

        public TChannelServerBuilder setBossGroup(EventLoopGroup bossGroup) {
            this.bossGroup = bossGroup;
            return this;
        }

        public TChannelServerBuilder setChildGroup(EventLoopGroup childGroup) {
            this.childGroup = childGroup;
            return this;
        }

        public TChannelServerBuilder setWorkerGroup(EventLoopGroup childGroup) {
            this.workerGroup = workerGroup;
            return this;
        }

        public TChannelServerBuilder setLogLevel(LogLevel logLevel) {
            this.logLevel = logLevel;
            return this;
        }

        public TChannel build() {
            if (bossGroup == null) {
                bossGroup = new NioEventLoopGroup();
            }
            if (childGroup == null) {
                childGroup = new NioEventLoopGroup();
            }
            if (workerGroup == null) {
                workerGroup = new NioEventLoopGroup();
            }
            if (logLevel == null) {
                logLevel = LogLevel.INFO;
            }

            ServerBootstrap serverBootstrap = this.serverBootstrap();

            return new TChannel(
                    this.service,
                    this.port,
                    serverBootstrap
            );
        }

        private ServerBootstrap serverBootstrap() {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(this.bossGroup, this.childGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(logLevel))
                    .childHandler(this.channelInitializer())
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .validate();
            return bootstrap;
        }

        private ChannelInitializer<SocketChannel> channelInitializer() {
            return new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    // Translates TCP Streams to Raw Frames
                    ch.pipeline().addLast(new TChannelLengthFieldBasedFrameDecoder());

                    // Translates Raw Frames into TFrames
                    ch.pipeline().addLast(new TFrameCodec());

                    // Translates TFrames into Messages
                    ch.pipeline().addLast(new MessageCodec());

                    // Handles Protocol Handshake
                    ch.pipeline().addLast(new InitRequestHandler());

                    // Handles Call Request RPC
                    ch.pipeline().addLast(new MessageMultiplexer());

                    // Pass RequestHandlers to the RequestDispatcher
                    ch.pipeline().addLast(
                            workerGroup,
                            new RequestDispatcher(requestHandlers)
                    );
                }
            };
        }

    }
}

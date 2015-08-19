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

import com.uber.tchannel.channels.ChannelManager;
import com.uber.tchannel.channels.ChannelRegistrar;
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.codecs.TChannelLengthFieldBasedFrameDecoder;
import com.uber.tchannel.codecs.TFrameCodec;
import com.uber.tchannel.handlers.InitRequestHandler;
import com.uber.tchannel.handlers.InitRequestInitiator;
import com.uber.tchannel.handlers.MessageMultiplexer;
import com.uber.tchannel.handlers.RequestRouter;
import com.uber.tchannel.handlers.ResponseRouter;
import com.uber.tchannel.headers.TransportHeaders;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Promise;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public final class TChannel {

    private final String service;
    private final ServerBootstrap serverBootstrap;
    private final Bootstrap clientBootstrap;
    private final ChannelManager channelManager;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup childGroup;
    private final InetAddress host;
    private final int port;

    private TChannel(Builder builder) {
        this.service = builder.service;
        this.serverBootstrap = builder.serverBootstrap();
        this.clientBootstrap = builder.bootstrap();
        this.channelManager = builder.channelManager;
        this.bossGroup = builder.bossGroup;
        this.childGroup = builder.childGroup;
        this.host = builder.host;
        this.port = builder.port;
    }

    public InetAddress getHost() {
        return host;
    }

    public int getServerPort() {
        return port;
    }

    public ChannelFuture listen() throws InterruptedException {
        return this.serverBootstrap.bind(this.host, this.port).sync();
    }

    public void shutdown() throws InterruptedException {
        this.channelManager.close();
        this.bossGroup.shutdownGracefully();
        this.childGroup.shutdownGracefully();
    }

    public <T, U> Promise<Response<T>> call(
            InetAddress host,
            int port,
            Request<U> request,
            Class<T> responseType
    ) throws InterruptedException {

        // Set the 'cn' header
        request.getTransportHeaders().put(TransportHeaders.CALLER_NAME_KEY, this.service);

        // Get an outbound channel
        Channel ch = this.channelManager.findOrNew(new InetSocketAddress(host, port), this.clientBootstrap);

        // Get a response router for our outbound channel
        ResponseRouter responseRouter = ch.pipeline().get(ResponseRouter.class);

        // Ask the router to make a call on our behalf, and return its promise
        return responseRouter.expectResponse(request, responseType);

    }

    public static class Builder {

        private final String service;
        private final ChannelManager channelManager = new ChannelManager();
        private final InetAddress host;
        private int port = 0;
        private Map<String, RequestHandler> requestHandlers = new HashMap<>();
        private EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        private EventLoopGroup childGroup = new NioEventLoopGroup();
        private LogLevel logLevel = LogLevel.INFO;

        public Builder(String service) throws UnknownHostException {
            if (service == null) {
                throw new NullPointerException("`service` cannot be null");
            }
            this.service = service;
            this.host = InetAddress.getLocalHost();
        }

        public Builder setServerPort(int port) throws UnknownHostException {
            this.port = port;
            return this;
        }

        public Builder register(String endpoint, RequestHandler requestHandler) {
            requestHandlers.put(endpoint, requestHandler);
            return this;
        }

        public Builder setBossGroup(EventLoopGroup bossGroup) {
            this.bossGroup = bossGroup;
            return this;
        }

        public Builder setChildGroup(EventLoopGroup childGroup) {
            this.childGroup = childGroup;
            return this;
        }

        public Builder setLogLevel(LogLevel logLevel) {
            this.logLevel = logLevel;
            return this;
        }

        public TChannel build() {
            return new TChannel(this);
        }

        private Bootstrap bootstrap() {
            return new Bootstrap()
                    .group(this.childGroup)
                    .channel(NioSocketChannel.class)
                    .handler(this.channelInitializer(false))
                    .validate();
        }

        private ServerBootstrap serverBootstrap() {
            return new ServerBootstrap()
                    .group(this.bossGroup, this.childGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(logLevel))
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childHandler(this.channelInitializer(true))
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .validate();
        }

        private ChannelInitializer<SocketChannel> channelInitializer(final boolean isServer) {
            return new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    // Translates TCP Streams to Raw Frames
                    ch.pipeline().addLast("FrameDecoder", new TChannelLengthFieldBasedFrameDecoder());

                    // Translates Raw Frames into TFrames
                    ch.pipeline().addLast("TFrameCodec", new TFrameCodec());

                    // Translates TFrames into Messages
                    ch.pipeline().addLast("MessageCodec", new MessageCodec());

                    if (isServer) {
                        ch.pipeline().addLast("InitRequestHandler", new InitRequestHandler());
                    } else {
                        ch.pipeline().addLast("InitRequestInitiator", new InitRequestInitiator());
                    }

                    // Handles Call Request RPC
                    ch.pipeline().addLast("MessageMultiplexer", new MessageMultiplexer());

                    // Pass RequestHandlers to the RequestRouter
                    ch.pipeline().addLast("RequestRouter", new RequestRouter(requestHandlers));

                    ch.pipeline().addLast("ResponseRouter", new ResponseRouter());

                    // Register Channels as they are created.
                    ch.pipeline().addLast("ChannelRegistrar", new ChannelRegistrar(channelManager));

                }
            };
        }

    }
}

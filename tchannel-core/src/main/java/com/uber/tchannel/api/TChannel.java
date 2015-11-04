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

import com.uber.tchannel.channels.Connection;
import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.channels.ChannelRegistrar;
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.codecs.TChannelLengthFieldBasedFrameDecoder;
import com.uber.tchannel.codecs.TFrameCodec;
import com.uber.tchannel.handlers.InitRequestHandler;
import com.uber.tchannel.handlers.InitRequestInitiator;
import com.uber.tchannel.handlers.MessageDefragmenter;
import com.uber.tchannel.handlers.MessageFragmenter;
import com.uber.tchannel.handlers.PingHandler;
import com.uber.tchannel.handlers.RequestRouter;
import com.uber.tchannel.handlers.ResponseRouter;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.messages.JSONSerializer;
import com.uber.tchannel.messages.Serializer;
import com.uber.tchannel.messages.ThriftSerializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public final class TChannel {

    private final String service;
    private final ServerBootstrap serverBootstrap;
    private final PeerManager peerManager;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup childGroup;
    private final InetAddress host;
    private final int port;
    private String listeningHost = "0.0.0.0";
    private int listeningPort;
    private ExecutorService exectorService;
    private final int maxQueuedRequests;
    private final int initTimeout;

    private Map<String, SubChannel> subChannels = new HashMap<>();

    private final Serializer serializer = new Serializer(new HashMap<ArgScheme, Serializer.SerializerInterface>() {
        {
            put(ArgScheme.JSON, new JSONSerializer());
            put(ArgScheme.THRIFT, new ThriftSerializer());
        }
    });

    private TChannel(Builder builder) {
        this.service = builder.service;
        this.exectorService = builder.executorService;
        this.serverBootstrap = builder.serverBootstrap(this);
        this.bossGroup = builder.bossGroup;
        this.childGroup = builder.childGroup;
        this.host = builder.host;
        this.port = builder.port;
        this.maxQueuedRequests = builder.maxQueuedRequests;
        this.initTimeout = builder.initTimeout;
        this.peerManager = new PeerManager(builder.bootstrap(this));
    }

    public int getListeningPort() {
        return listeningPort;
    }

    public InetAddress getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getServiceName() {
        return this.service;
    }

    public PeerManager getPeerManager() {
        return this.peerManager;
    }

    public long getInitTimeout() {
        return this.initTimeout;
    }

    public ChannelFuture listen() throws InterruptedException {
        ChannelFuture f = this.serverBootstrap.bind(this.host, this.port).sync();
        InetSocketAddress localAddress = (InetSocketAddress) f.channel().localAddress();
        this.listeningPort = localAddress.getPort();
        this.listeningHost = localAddress.getHostName();
        this.peerManager.setHostPort(String.format("%s:%d", this.listeningHost, this.listeningPort));
        return f;
    }

    public SubChannel getSubChannel(String service) {
        return subChannels.get(service);
    }

    // TODO: make sure makeSubChannel is called before listening ...
    public SubChannel makeSubChannel(String service, Connection.Direction preferredDirection) {
        SubChannel subChannel = getSubChannel(service);
        if (subChannel == null) {
            subChannel = new SubChannel(service, this, preferredDirection);
            subChannels.put(service, subChannel);
        }

        return subChannel;
    }

    public SubChannel makeSubChannel(String service) {
        return this.makeSubChannel(service, Connection.Direction.NONE);
    }

    public void shutdown(boolean sync) throws InterruptedException, ExecutionException {
        this.peerManager.close();
        Future bg = this.bossGroup.shutdownGracefully();
        Future cg = this.childGroup.shutdownGracefully();

        if (sync) {
            bg.get();
            cg.get();
        }
    }

    public void shutdown() throws InterruptedException, ExecutionException {
        this.shutdown(true);
    }

    public static class Builder {

        private final String service;
        private ExecutorService executorService = new ForkJoinPool();
        private int maxQueuedRequests = Runtime.getRuntime().availableProcessors() * 5;
        private InetAddress host;
        private int port = 0;
        private EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        private EventLoopGroup childGroup = new NioEventLoopGroup();
        private LogLevel logLevel = LogLevel.INFO;
        private int initTimeout = 2000;

        public Builder(String service) throws UnknownHostException {
            if (service == null) {
                throw new NullPointerException("`service` cannot be null");
            }
            this.service = service;
            this.host = InetAddress.getLocalHost();
        }

        public Builder setExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public Builder setMaxQueuedRequests(int maxQueuedRequests) {
            this.maxQueuedRequests = maxQueuedRequests;
            return this;
        }

        public Builder setServerHost(InetAddress host) {
            this.host = host;
            return this;
        }

        public Builder setServerPort(int port) throws UnknownHostException {
            this.port = port;
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

        public Builder setInitTimeout(int initTimeout) {
            this.initTimeout = initTimeout;
            return this;
        }

        public TChannel build() {
            return new TChannel(this);
        }

        private Bootstrap bootstrap(TChannel topChannel) {
            return new Bootstrap()
                .group(this.childGroup)
                .channel(NioSocketChannel.class)
                .handler(this.channelInitializer(false, topChannel))
//                .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
//                .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
                .validate();
        }

        private ServerBootstrap serverBootstrap(TChannel topChannel) {
            return new ServerBootstrap()
                .group(this.bossGroup, this.childGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(logLevel))
                .option(ChannelOption.SO_BACKLOG, 128)
                .childHandler(this.channelInitializer(true, topChannel))
                .childOption(ChannelOption.SO_KEEPALIVE, true)
//                .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
//                .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
                .validate();
        }

        private ChannelInitializer<SocketChannel> channelInitializer(final boolean isServer,
                                                                     final TChannel topChannel) {
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
                        ch.pipeline().addLast("InitRequestHandler",
                            new InitRequestHandler(topChannel.getPeerManager()));
                    } else {
                        ch.pipeline().addLast("InitRequestInitiator",
                            new InitRequestInitiator(topChannel.getPeerManager()));
                    }

                    // Handle PingRequestFrame
                    ch.pipeline().addLast("PingHandler", new PingHandler());

                    // Handles Call Request RPC
                    ch.pipeline().addLast("MessageDefragmenter", new MessageDefragmenter());
                    ch.pipeline().addLast("MessageFragmenter", new MessageFragmenter());

                    // Pass RequestHandlers to the RequestRouter
                    ch.pipeline().addLast("RequestRouter", new RequestRouter(
                        topChannel, executorService, maxQueuedRequests));

                    ch.pipeline().addLast("ResponseRouter", new ResponseRouter());

                    // Register Channels as they are created.
                    ch.pipeline().addLast("ChannelRegistrar", new ChannelRegistrar(topChannel.getPeerManager()));

                }
            };
        }
    }
}

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

import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.channels.ChannelRegistrar;
import com.uber.tchannel.channels.Connection;
import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.codecs.TChannelLengthFieldBasedFrameDecoder;
import com.uber.tchannel.handlers.InitRequestHandler;
import com.uber.tchannel.handlers.InitRequestInitiator;
import com.uber.tchannel.handlers.MessageDefragmenter;
import com.uber.tchannel.handlers.MessageFragmenter;
import com.uber.tchannel.handlers.RequestRouter;
import com.uber.tchannel.handlers.ResponseRouter;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.tracing.TracingContext;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.HashedWheelTimer;
import io.opentracing.Tracer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public final class TChannel {

    private static final Logger logger = LoggerFactory.getLogger(TChannel.class);

    private final HashedWheelTimer timer;

    private final @NotNull String service;
    private final ServerBootstrap serverBootstrap;
    private final @NotNull PeerManager peerManager;
    private final @NotNull EventLoopGroup bossGroup;
    private final @NotNull EventLoopGroup childGroup;
    private final InetAddress host;
    private final int port;
    private String listeningHost = "0.0.0.0";
    private int listeningPort;
    private final long initTimeout;
    private final int resetOnTimeoutLimit;
    private final int clientMaxPendingRequests;
    private final Tracer tracer;
    private final TracingContext tracingContext;

    private final @NotNull Map<String, SubChannel> subChannels = new HashMap<>();
    private @Nullable RequestHandler defaultUserHandler;
    private @Nullable SimpleChannelInboundHandler<Request> customRequestRouter;

    private TChannel(@NotNull Builder builder) {
        this.service = builder.service;
        this.serverBootstrap = builder.serverBootstrap(this);
        this.bossGroup = builder.bossGroup;
        this.childGroup = builder.childGroup;
        this.host = builder.host;
        this.port = builder.port;
        this.initTimeout = builder.initTimeout;
        this.resetOnTimeoutLimit = builder.resetOnTimeoutLimit;
        this.peerManager = new PeerManager(builder.bootstrap(this));
        this.timer = builder.timer;
        this.clientMaxPendingRequests = builder.clientMaxPendingRequests;
        this.tracer = builder.tracer;
        this.tracingContext = builder.tracingContext;
    }

    public String getListeningHost() {
        return listeningHost;
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

    public @NotNull String getServiceName() {
        return service;
    }

    public @NotNull PeerManager getPeerManager() {
        return this.peerManager;
    }

    public int getResetOnTimeoutLimit() {
        return resetOnTimeoutLimit;
    }

    public long getInitTimeout() {
        return this.initTimeout;
    }

    public boolean isListening() {
        return !"0.0.0.0".equals(listeningHost);
    }

    public Tracer getTracer() { return tracer; }

    public TracingContext getTracingContext() { return tracingContext; }

    public @NotNull ChannelFuture listen() throws InterruptedException {
        ChannelFuture f = this.serverBootstrap.bind(this.host, this.port).sync();
        InetSocketAddress localAddress = (InetSocketAddress) f.channel().localAddress();
        this.listeningPort = localAddress.getPort();
        this.listeningHost = localAddress.getAddress().getHostAddress();
        this.peerManager.setHostPort(String.format("%s:%d", this.listeningHost, this.listeningPort));
        return f;
    }

    public void setDefaultUserHandler(@Nullable RequestHandler requestHandler) {
        this.defaultUserHandler = requestHandler;
    }

    public @Nullable SubChannel getSubChannel(String service) {
        return subChannels.get(service);
    }

    public @NotNull SubChannel makeSubChannel(String service, Connection.Direction preferredDirection) {
        if (isListening()) {
            logger.warn("makeSubChannel should be called before listen - service: {}", service);
        }

        SubChannel subChannel = getSubChannel(service);
        if (subChannel == null) {
            subChannel = new SubChannel(service, this, preferredDirection);
            subChannels.put(service, subChannel);
        }

        return subChannel;
    }

    public @NotNull SubChannel makeSubChannel(String service) {
        return this.makeSubChannel(service, Connection.Direction.NONE);
    }

    public void shutdown(boolean sync) {
        timer.stop();
        peerManager.close();
        Future<?> bg = bossGroup.shutdownGracefully();
        Future<?> cg = childGroup.shutdownGracefully();

        try {
            if (sync) {
                bg.get();
                cg.get();
            }
        } catch (InterruptedException ie) {
            // set interrupt flag
            Thread.currentThread().interrupt();
            logger.warn("shutdown interrupted.", ie);
        } catch (ExecutionException ee) {
            logger.warn("shutdown runs into an ExecutionException.", ee);
        }
    }

    public void shutdown() {
        shutdown(true);
    }

    public int getClientMaxPendingRequests() {
        return clientMaxPendingRequests;
    }

    public @Nullable RequestHandler getDefaultUserHandler() {
        return defaultUserHandler;
    }

    public @Nullable SimpleChannelInboundHandler<Request> getCustomRequestRouter() {
        return customRequestRouter;
    }

    public void setCustomRequestRouter(@Nullable SimpleChannelInboundHandler<Request> customRequestRouter) {
        this.customRequestRouter = customRequestRouter;
    }

    public static class Builder {

        private final @NotNull HashedWheelTimer timer;
        private static ExecutorService executorService = new ForkJoinPool();
        private @NotNull EventLoopGroup bossGroup;
        private @NotNull EventLoopGroup childGroup;

        private static final boolean useEpoll = Epoll.isAvailable();

        private final @NotNull String service;
        private InetAddress host;
        private int port = 0;

        private long initTimeout = -1;
        private int resetOnTimeoutLimit = Integer.MAX_VALUE;
        private int clientMaxPendingRequests = 100000;
        private static final int WRITE_BUFFER_LOW_WATER_MARK = 8 * 1024;
        private static final int WRITE_BUFFER_HIGH_WATER_MARK = 32 * 1024;

        private Tracer tracer;
        private TracingContext tracingContext = new TracingContext.Default();

        public Builder(@NotNull String service) {
            if (service == null) {
                throw new NullPointerException("`service` cannot be null");
            }

            this.service = service;
            this.host = TChannelUtilities.getCurrentIp();
            if (this.host == null) {
                logger.error("failed to get current IP");
            }

            timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS); // FIXME this is premature and may leak timer
            bossGroup = useEpoll ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
            childGroup = useEpoll ? new EpollEventLoopGroup() : new NioEventLoopGroup();
        }

        public @NotNull Builder setExecutorService(ExecutorService executorService) {
            Builder.executorService = executorService;
            return this;
        }

        public @NotNull Builder setClientMaxPendingRequests(int clientMaxPendingRequests) {
            this.clientMaxPendingRequests = clientMaxPendingRequests;
            return this;
        }

        public @NotNull Builder setServerHost(InetAddress host) {
            this.host = host;
            return this;
        }

        public @NotNull Builder setServerPort(int port) {
            this.port = port;
            return this;
        }

        public @NotNull Builder setBossGroup(@NotNull EventLoopGroup bossGroup) {
            this.bossGroup = bossGroup;
            return this;
        }

        public @NotNull Builder setChildGroup(@NotNull EventLoopGroup childGroup) {
            this.childGroup = childGroup;
            return this;
        }

        public @NotNull Builder setInitTimeout(long initTimeout) {
            this.initTimeout = initTimeout;
            return this;
        }

        public @NotNull Builder setResetOnTimeoutLimit(int resetOnTimeoutLimit) {
            this.resetOnTimeoutLimit = resetOnTimeoutLimit;
            return this;
        }

        public @NotNull Builder setTracer(Tracer tracer) {
            this.tracer = tracer;
            return this;
        }

        public @NotNull Builder setTracingContext(TracingContext tracingContext) {
            this.tracingContext = tracingContext;
            return this;
        }

        public @NotNull TChannel build() {
            logger.debug(useEpoll ? "Using native epoll transport" : "Using NIO transport");
            return new TChannel(this);
        }

        private @NotNull Bootstrap bootstrap(@NotNull TChannel topChannel) {
            return new Bootstrap()
                .group(this.childGroup)
                .channel(useEpoll ? EpollSocketChannel.class : NioSocketChannel.class)
                .handler(this.channelInitializer(false, topChannel))
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(WRITE_BUFFER_LOW_WATER_MARK, WRITE_BUFFER_HIGH_WATER_MARK))
                .validate();
        }

        private @NotNull ServerBootstrap serverBootstrap(@NotNull TChannel topChannel) {
            return new ServerBootstrap()
                .group(this.bossGroup, this.childGroup)
                .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .option(ChannelOption.SO_BACKLOG, 128)
                .childHandler(this.channelInitializer(true, topChannel))
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(
                    ChannelOption.WRITE_BUFFER_WATER_MARK,
                    new WriteBufferWaterMark(WRITE_BUFFER_LOW_WATER_MARK, WRITE_BUFFER_HIGH_WATER_MARK)
                )
                .validate();
        }

        private @NotNull ChannelInitializer<SocketChannel> channelInitializer(
            final boolean isServer, final @NotNull TChannel topChannel
        ) {
            return new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(@NotNull SocketChannel ch) throws Exception {
                    // Translates TCP Streams to Raw Frames
                    ch.pipeline().addLast("FrameDecoder", new TChannelLengthFieldBasedFrameDecoder());

                    // Translates Raw Frames into TFrames
                    // ch.pipeline().addLast("TFrameCodec", new TFrameCodec());

                    // Translates TFrames into Messages
                    // ch.pipeline().addLast("MessageCodec", new MessageCodec());

                    if (isServer) {
                        ch.pipeline().addLast("InitRequestHandler",
                            new InitRequestHandler(topChannel.getPeerManager()));
                    } else {
                        ch.pipeline().addLast("InitRequestInitiator",
                            new InitRequestInitiator(topChannel.getPeerManager()));
                    }

                    // Ping frame is not current used
                    // Handle PingRequestFrame
                    // ch.pipeline().addLast("PingHandler", new PingHandler());

                    // Handles Call Request RPC
                    ch.pipeline().addLast("MessageDefragmenter", new MessageDefragmenter());
                    ch.pipeline().addLast("MessageFragmenter", new MessageFragmenter());

                    // Pass RequestHandlers to the RequestRouter
                    ch.pipeline().addLast(
                        "RequestRouter",
                        topChannel.getCustomRequestRouter() != null
                            ? topChannel.getCustomRequestRouter()
                            : new RequestRouter(topChannel, executorService)
                    );
                    ch.pipeline().addLast("ResponseRouter", new ResponseRouter(topChannel, timer));

                    // Register Channels as they are created.
                    ch.pipeline().addLast("ChannelRegistrar", new ChannelRegistrar(topChannel.getPeerManager()));

                }
            };
        }

    }

}

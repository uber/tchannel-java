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
import com.uber.tchannel.schemes.DefaultRawRequestHandler;
import com.uber.tchannel.schemes.RawRequestHandler;
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
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public final class TChannel {

    private final String service;
    private final ServerBootstrap serverBootstrap;
    private final Bootstrap clientBootstrap;
    private final ChannelManager channelManager;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup childGroup;
    private final InetSocketAddress address;

    private TChannel(Builder builder) {
        this.service = builder.service;
        this.serverBootstrap = builder.serverBootstrap();
        this.clientBootstrap = builder.bootstrap();
        this.channelManager = builder.channelManager;
        this.bossGroup = builder.bossGroup;
        this.childGroup = builder.childGroup;
        this.address = builder.address;
    }

    public ChannelFuture listen() throws InterruptedException {
        return this.serverBootstrap.bind(this.address).sync();
    }

    public void shutdown() throws InterruptedException {
        this.channelManager.close();
        this.bossGroup.shutdownGracefully();
        this.childGroup.shutdownGracefully();
    }

    /**
     * Makes a Request to the remote address.
     * <p/>
     * This will attempt to reuse a connection, i.e. TCP socket, to a remote address and create one if needed.
     *
     * @param address the remote address to make the request
     * @param request the request object to send
     * @return a promise representing a future response from this request
     * @throws InterruptedException
     */
    public Promise<Response> request(final InetSocketAddress address,
                                     final Request request) throws InterruptedException {

        // Get a channel for this request
        Channel ch = this.channelManager.findOrNew(address, this.clientBootstrap);

        // Write the request
        ch.write(request);

        // Prepare for a response.

        // Create  a new response promise to hand to the client.
        // TODO: on which EventExecutor should Promises be fulfilled?
        Promise<Response> responsePromise = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);

        // Get a handle to the responseRouter to tell it that it should expet a response
        ResponseRouter responseRouter = ch.pipeline().get(ResponseRouter.class);

        // Let the ResponseDispatch handler know that it should handle this response
        responseRouter.expect(request.getId(), responsePromise);

        // Flush and return the response promise
        // TODO: when and how often should we flush requests?
        ch.flush();
        return responsePromise;

    }

    public static class Builder {

        private final String service;
        private final ChannelManager channelManager = new ChannelManager();
        private InetSocketAddress address;
        private RawRequestHandler rawRequestHandler;
        private Map<String, RequestHandler> requestHandlers = new HashMap<>();
        private EventLoopGroup bossGroup;
        private EventLoopGroup childGroup;
        private LogLevel logLevel;

        public Builder(String service) {
            if (service == null) {
                throw new NullPointerException("`service` cannot be null");
            }
            this.service = service;
        }

        public Builder setPort(int port) {
            this.address = new InetSocketAddress(port);
            return this;
        }

        public Builder register(String service, RequestHandler requestHandler) {
            requestHandlers.put(service, requestHandler);
            return this;
        }

        public Builder registerRawRequestHandler(RawRequestHandler rawRequestHandler) {
            this.rawRequestHandler = rawRequestHandler;
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
            if (address == null) {
                address = new InetSocketAddress(0);
            }
            if (bossGroup == null) {
                bossGroup = new NioEventLoopGroup();
            }
            if (childGroup == null) {
                childGroup = new NioEventLoopGroup();
            }
            if (logLevel == null) {
                logLevel = LogLevel.INFO;
            }
            if (rawRequestHandler == null) {
                rawRequestHandler = new DefaultRawRequestHandler();
            }
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
                    ch.pipeline().addLast("RequestRouter", new RequestRouter(requestHandlers, rawRequestHandler));

                    ch.pipeline().addLast("ResponseRouter", new ResponseRouter());

                    // Register Channels as they are created.
                    ch.pipeline().addLast("ChannelRegistrar", new ChannelRegistrar(channelManager));

                }
            };
        }

    }
}

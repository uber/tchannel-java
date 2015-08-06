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
import com.uber.tchannel.handlers.RequestDispatcher;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.HashMap;
import java.util.Map;

public class TChannelServerBuilder {

    private final String service;
    private int port;
    private Map<String, RequestHandler> requestHandlers = new HashMap<String, RequestHandler>();
    private EventLoopGroup bossGroup;
    private EventLoopGroup childGroup;

    public TChannelServerBuilder(String service) {
        if (service == null) {
            throw new NullPointerException("`service` cannot be null");
        }
        this.service = service;
    }

    public TChannelServerBuilder port(int port) {
        this.port = port;
        return this;
    }

    public TChannelServerBuilder register(String service, RequestHandler requestHandler) {
        this.requestHandlers.put(service, requestHandler);
        return this;
    }

    public TChannelServerBuilder bossGroup(EventLoopGroup bossGroup) {
        this.bossGroup = bossGroup;
        return this;
    }

    public TChannelServerBuilder childGroup(EventLoopGroup childGroup) {
        this.childGroup = childGroup;
        return this;
    }

    public TChannel build() {
        if (bossGroup == null) {
            bossGroup = new NioEventLoopGroup();
        }
        if (childGroup == null) {
            childGroup = new NioEventLoopGroup();
        }

        ServerBootstrap serverBootstrap = this.serverBootstrap();

        return new TChannel(
                this.service,
                this.port,
                serverBootstrap
        );
    }

    private ChannelInitializer<SocketChannel> channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                // Translates TCP Streams to Raw Frames
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(
                        TFrame.MAX_FRAME_LENGTH,
                        TFrame.LENGTH_FIELD_OFFSET,
                        TFrame.LENGTH_FIELD_LENGTH,
                        TFrame.LENGTH_ADJUSTMENT,
                        TFrame.INITIAL_BYTES_TO_STRIP,
                        TFrame.FAIL_FAST
                ));

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
                        new NioEventLoopGroup(),
                        new RequestDispatcher(requestHandlers)
                );
            }
        };
    }

    private ServerBootstrap serverBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(this.bossGroup, this.childGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(this.channelInitializer())
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .validate();
        return bootstrap;
    }

}
